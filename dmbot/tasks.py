from datetime import timedelta
import logging
import redis
from celery import shared_task, current_app
from celery.result import AsyncResult
from django.conf import settings
from django.db.models import Q
from django.utils import timezone
import logging
import random
import time

from instagrapi.exceptions import RateLimitError, ClientError

from .filter import UserFilter
from .models import ScrapedUser, Account, DMCampaign, Alert
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .utils import setup_client, send_alert

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)

@shared_task(bind=True, ignore_result=False)
def scrape_users_task(self, account_id, source_type, source_id, amount=None):
    from django.core.cache import cache
    import logging

    lock_key = "scraping_lock"
    account = Account.objects.get(id=account_id)
    #
    # # --- CLEAR ALL REDIS CACHE BEFORE START ---
    # try:
    #     redis_client.flushall()  # ⚠️ Clears everything in Redis (be careful in production)
    #     cache.clear()  # Clears Django-level cache if using cache framework
    #     logging.info("✅ All Redis and Django caches cleared before scraping.")
    # except Exception as e:
    #     logging.error(f"❌ Failed to clear cache: {e}")
    #     send_alert(
    #         message=f"Failed to clear cache: {e}",
    #         severity='error',
    #         account=account
    #     )

    if redis_client.get(lock_key):
        logging.warning("Scraping task already running, terminating to start new task...")
        send_alert(
            message="Scraping task scheduled: New scraping task is now active.",
            severity='warning',
            account=account
        )
        redis_client.delete("scraping_lock")

        # return {"status": "skipped", "reason": "Another scraping task is active"}
    try:
        # Acquire lock
        redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
        account.status = "scraping"
        account.task_id = self.request.id  # Store task_id
        account.save()

        logging.info(f"Started scraping {source_type}:{source_id} with account {account.username}")
        send_alert(
            message=f"Started scraping {source_type}:{source_id} with account {account.username}",
            severity='info',
            account=account
        )

        scraper = InstagramScraper()
        usernames = scraper.collect_usernames(account, source_type, source_id, amount)

        # Save scraped users (uncomment if needed)
        # for username in usernames:
        #     ScrapedUser.objects.create(
        #         username=username,
        #         account=account,
        #         source_type=source_type,
        #         source_value=source_id,
        #         scraped_at=timezone.now()
        #     )

        account.last_active = timezone.now()
        account.actions_this_hour += 1
        account.update_health_score(action_success=True, action_type='scrape')
        account.status = "idle"
        account.task_id = None  # Clear task_id
        account.save()

        logging.info(f"Completed: {len(usernames)} usernames collected with account {account.username}")
        send_alert(
            message=f"Completed: {len(usernames)} usernames collected with account {account.username}",
            severity='info',
            account=account
        )

        return {"status": "success", "usernames_collected": len(usernames)}

    except Exception as e:
        logging.error(f"Scrape task failed for account {account_id}: {str(e)}", exc_info=True)
        account = Account.objects.get(id=account_id)
        send_alert(
            message=f"Scrape task failed for account {account.username}: {str(e)}",
            severity='error',
            account=account
        )
        account.status = "idle"
        account.task_id = None  # Clear task_id
        account.update_health_score(action_success=False, action_type='scrape')
        account.save()
        raise

    finally:
        redis_client.delete(lock_key)

@shared_task
def send_dms_task(campaign_id, max_dms_per_account=15):
    dm_sender = DMSender()
    try:
        dm_sender.send_dms_for_campaign(campaign_id, max_dms_per_account)
        logging.info(f"DM campaign {campaign_id} completed")
    except Exception as e:
        logging.error(f"DM campaign {campaign_id} failed: {e}")

@shared_task(bind=True, rate_limit='200/h')
def enrich_user_details_task(self):
    """Fetch detailed metadata for users in batches using any idle account"""
    lock_key = "enrichment_lock"
    if redis_client.get(lock_key):
        logging.warning("Enrichment task already running, skipping new task")
        send_alert("Enrichment task skipped: another enrichment task is active", "warning")
        return 0

    try:
        redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
        logging.info("Starting enrich_user_details_task")
        send_alert("Starting enrich_user_details_task", "info")

        accounts_qs = Account.objects.filter(
            status='idle',
            warmed_up=True,
            health_score__gte=50
        )

        count = accounts_qs.count()
        if count <= 1:
            accounts = accounts_qs
        elif count == 2:
            accounts = accounts_qs[:1]
        elif count == 3:
            accounts = accounts_qs[:2]
        else:
            accounts = accounts_qs[: count // 2]

        logging.info(f"Found {accounts.count()} idle accounts for enrichment")
        send_alert(f"Found {accounts.count()} idle accounts for enrichment", "info")

        if not accounts:
            logging.warning("No idle, warmed-up accounts available for enrichment")
            send_alert("No idle, warmed-up accounts available for enrichment", "warning")
            return 0

        updated_count = 0
        skipped_count = 0
        batch_size = 50

        for account in accounts:
            if not account.can_scrape():
                logging.info(f"Account {account.username} cannot enrich users (health: {account.health_score}%)")
                send_alert(f"Account {account.username} cannot enrich users (health: {account.health_score}%)", "info", account)
                continue

            cl = setup_client(account)
            if not cl:
                logging.error(f"Failed to setup client for {account.username}")
                send_alert(f"Failed to setup client for {account.username}", "error", account)
                account.update_health_score(False, "scrape")
                account.save()
                continue

            user_batch = ScrapedUser.objects.filter(
                account=account,
                details_fetched=False,
                failure_reason__isnull=True
            )[:batch_size]
            logging.info(f"Processing {user_batch.count()} users for account {account.username}")
            send_alert(f"Processing {user_batch.count()} users for account {account.username}", "info", account)

            if not user_batch:
                logging.info(f"No users to enrich for account {account.username}")
                send_alert(f"No users to enrich for account {account.username}", "info", account)
                continue

            try:
                account.status = "enriching_users"
                account.task_id = self.request.id  # Store task_id
                account.save()
                scraper = InstagramScraper()
                for user in user_batch:
                    try:
                        user_data = scraper._get_user_detailed_info(cl, user.username)
                        if not user_data:
                            user.failure_reason = "Failed to fetch detailed info"
                            user.is_active = False
                            user.details_fetched = True
                            user.save()
                            skipped_count += 1
                            logging.warning(f"No data for user {user.username}")
                            send_alert(f"No data for user {user.username}", "warning", account)
                            continue

                        user.user_id = user_data.get("user_id")
                        user.biography = user_data.get("biography", "")
                        user.follower_count = user_data.get("follower_count", 0)
                        user.following_count = user_data.get("following_count", 0)
                        user.post_count = user_data.get("post_count", 0)
                        user.last_post_date = user_data.get("last_post_date")
                        user.is_active = user_data.get("is_active", True)
                        user.details_fetched = True
                        user.save()
                        updated_count += 1
                        logging.info(f"Enriched user {user.username} with account {account.username}")
                        send_alert(f"Enriched user {user.username} with account {account.username}", "info", account)

                        time.sleep(random.uniform(25, 30))
                        if updated_count % 10 == 0:
                            time.sleep(random.uniform(120, 300))

                    except RateLimitError:
                        logging.warning(f"Rate limit hit for {account.username} during enrichment")
                        send_alert(f"Rate limit hit for {account.username} during enrichment", "warning", account)
                        account.status = 'rate_limited'
                        account.task_id = None  # Clear task_id
                        account.save()
                        break
                    except ClientError as e:
                        logging.error(f"Failed to enrich user {user.username}: {e}")
                        send_alert(f"Failed to enrich user {user.username}: {e}", "error", account)
                        user.failure_reason = str(e)
                        user.details_fetched = True
                        user.save()
                        skipped_count += 1
                        if 'challenge_required' in str(e).lower():
                            account.status = 'error'
                            account.login_failures += 1
                            account.last_login_failure = timezone.now()
                            account.task_id = None  # Clear task_id
                            account.save()
                            send_alert(f"Challenge required for {account.username}", "error", account)
                            break
                    except Exception as e:
                        logging.error(f"Unexpected error enriching user {user.username}: {e}", exc_info=True)
                        send_alert(f"Unexpected error enriching user {user.username}: {e}", "error", account)
                        user.failure_reason = str(e)
                        user.details_fetched = True
                        user.save()
                        skipped_count += 1
                        continue

                account.last_active = timezone.now()
                account.status = "idle"
                account.update_health_score(True, "scrape")
                account.task_id = None  # Clear task_id
                account.save()
                time.sleep(random.uniform(60, 120))

            except Exception as e:
                logging.error(f"Enrichment failed for account {account.username}: {e}", exc_info=True)
                send_alert(f"Enrichment failed for account {account.username}: {e}", "error", account)
                account.status = "idle"
                account.task_id = None  # Clear task_id
                account.update_health_score(False, "scrape")
                account.save()
                continue

        logging.info(f"Enriched {updated_count} users, skipped {skipped_count}")
        send_alert(f"Enriched {updated_count} users, skipped {skipped_count}", "info")

        if updated_count > 0:
            logging.info("Queuing classify_users_task")
            send_alert("Queuing classify_users_task", "info")
            classify_users_task.delay()

        if ScrapedUser.objects.filter(details_fetched=False, failure_reason__isnull=True).exists():
            logging.info("Queuing another enrich_user_details_task")
            send_alert("Queuing another enrich_user_details_task", "info")
            enrich_user_details_task.delay()

        return updated_count

    except Exception as e:
        logging.error(f"enrich_user_details_task failed: {e}", exc_info=True)
        send_alert(f"enrich_user_details_task failed: {e}", "error")
        raise

    finally:
        redis_client.delete(lock_key)

@shared_task(rate_limit='50/h')  # Limit to 50 calls per hour to avoid API overuse
def classify_users_task(batch_size=50):
    """Classify users for profession and country in batches"""
    try:
        user_filter = UserFilter()
        classified_count = user_filter.classify_users_ai(batch_size=batch_size)
        logging.info(f"Classified {classified_count} users in batch")

        # Schedule another batch if more users need classification
        if ScrapedUser.objects.filter(
                Q(profession='') | Q(country=''),
                biography__isnull=False
        ).exists():
            classify_users_task.delay(batch_size=batch_size)
    except Exception as e:
        logging.error(f"Classification task failed: {e}")

@shared_task
def warmup_accounts_task():
    scraper = InstagramScraper()
    try:
        accounts_to_warmup = Account.objects.filter(
            warmed_up=False,
            status__in=['idle', 'error'],
            health_score__gt=50
        )[:5]
        for account in accounts_to_warmup:
            try:
                scraper.warm_up_account(account)
                time.sleep(random.randint(300, 600))
            except Exception as e:
                logging.error(f"Warmup failed for {account.username}: {e}")
        logging.info(f"Warmup task completed for {len(accounts_to_warmup)} accounts")
    except Exception as e:
        logging.error(f"Warmup task failed: {e}")

@shared_task
def health_check_task():
    try:
        flagged_accounts = Account.objects.filter(health_score__lt=30)
        for account in flagged_accounts:
            Alert.objects.create(
                message=f"Account {account.username} has low health score: {account.health_score}%",
                severity='warning',
                account=account
            )
        banned_accounts = Account.objects.filter(status='banned')
        for account in banned_accounts:
            Alert.objects.create(
                message=f"Account {account.username} is banned",
                severity='critical',
                account=account
            )
        failed_accounts = Account.objects.filter(
            login_failures__gt=3,
            last_login_failure__gte=timezone.now() - timedelta(hours=24)
        )
        for account in failed_accounts:
            Alert.objects.create(
                message=f"Account {account.username} has {account.login_failures} login failures",
                severity='error',
                account=account
            )
        logging.info("Health check completed")
    except Exception as e:
        logging.error(f"Health check failed: {e}")

@shared_task
def cancel_task(task_id):
    try:
        result = AsyncResult(task_id)
        result.revoke(terminate=True)
        alert = Alert.objects.filter(message__contains=task_id).first()
        if alert:
            alert.message += "\nTask cancelled by user"
            alert.severity = 'warning'
            alert.save()
        redis_client.delete("scraping_lock")  # Release lock if scraping task
        logging.info(f"Task {task_id} cancelled")
        return {"status": "cancelled", "task_id": task_id}
    except Exception as e:
        logging.error(f"Failed to cancel task {task_id}: {str(e)}")
        Alert.objects.create(
            message=f"Failed to cancel task {task_id}: {str(e)}",
            severity='error',
            timestamp=timezone.now()
        )
        return {"status": "failed", "error": str(e)}

@shared_task(bind=True)
def clean_alerts_task(self):
    """Delete old Alert entries when count reaches 1000"""
    lock_key = "clean_alerts_lock"

    if redis_client.get(lock_key):
        logging.warning("Clean alerts task already running, skipping new task")
        send_alert("Clean alerts task skipped: another clean alerts task is active", "warning")
        return {"status": "skipped", "reason": "Another clean alerts task is active"}

    try:
        redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
        logging.info("Starting clean_alerts_task")
        send_alert("Starting clean_alerts_task", "info")

        alert_count = Alert.objects.count()
        if alert_count < 1000:
            logging.info(f"Alert count ({alert_count}) below 1000, no deletion needed")
            send_alert(f"Alert count ({alert_count}) below 1000, no deletion needed", "info")
            return {"status": "success", "deleted": 0}

        # Delete oldest alerts to bring count below 1000
        excess_count = alert_count - 900  # Keep 900 to leave buffer
        alerts_to_delete = Alert.objects.order_by('timestamp')[:excess_count]
        deleted_count = alerts_to_delete.count()
        alerts_to_delete.delete()

        logging.info(f"Deleted {deleted_count} old alerts")
        send_alert(f"Deleted {deleted_count} old alerts", "info")
        return {"status": "success", "deleted": deleted_count}

    except Exception as e:
        logging.error(f"clean_alerts_task failed: {str(e)}", exc_info=True)
        send_alert(f"clean_alerts_task failed: {str(e)}", "error")
        raise

    finally:
        redis_client.delete(lock_key)

@shared_task(bind=True)
def reset_stale_accounts_task(self):
    """Check accounts with non-idle status and reset to idle if task_id is not in Celery queue."""
    lock_key = "reset_stale_accounts_lock"
    if redis_client.get(lock_key):
        logging.warning("Reset stale accounts task already running, skipping")
        send_alert("Reset stale accounts task skipped: another instance is active", "warning")
        return 0

    try:
        redis_client.set(lock_key, self.request.id, ex=3600)
        logging.info("Starting reset_stale_accounts_task")
        send_alert("Starting reset_stale_accounts_task", "info")

        # Get non-idle accounts
        non_idle_accounts = Account.objects.exclude(status="idle")
        if not non_idle_accounts:
            logging.info("No non-idle accounts found")
            send_alert("No non-idle accounts found", "info")
            return 0

        # Inspect Celery queue
        inspector = current_app.control.inspect()
        active_tasks = inspector.active() or {}
        scheduled_tasks = inspector.scheduled() or {}
        reserved_tasks = inspector.reserved() or {}
        task_ids = set()
        for worker, tasks in active_tasks.items():
            task_ids.update(task['id'] for task in tasks)
        for worker, tasks in scheduled_tasks.items():
            task_ids.update(task['id'] for task in tasks)
        for worker, tasks in reserved_tasks.items():
            task_ids.update(task['id'] for task in tasks)

        reset_count = 0
        for account in non_idle_accounts:
            if account.task_id and account.task_id not in task_ids:
                logging.info(f"Resetting account {account.username}: status={account.status}, task_id={account.task_id}")
                send_alert(f"Resetting account {account.username}: status={account.status}, task_id={account.task_id}", "info", account)
                account.status = "idle"
                account.task_id = None
                account.save()
                reset_count += 1
            elif not account.task_id:
                logging.info(f"Resetting account {account.username}: status={account.status}, no task_id")
                send_alert(f"Resetting account {account.username}: status={account.status}, no task_id", "info", account)
                account.status = "idle"
                account.save()
                reset_count += 1

        logging.info(f"Completed reset_stale_accounts_task: {reset_count} accounts reset to idle")
        send_alert(f"Completed reset_stale_accounts_task: {reset_count} accounts reset to idle", "info")
        return reset_count

    except Exception as e:
        logging.error(f"Error in reset_stale_accounts_task: {e}", exc_info=True)
        send_alert(f"Error in reset_stale_accounts_task: {e}", "error")
        return 0

    finally:
        redis_client.delete(lock_key)

