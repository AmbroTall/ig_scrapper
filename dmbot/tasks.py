from datetime import timedelta

import redis
from celery import shared_task
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
    lock_key = "scraping_lock"
    if redis_client.get(lock_key):
        logging.warning("Scraping task already running, skipping new task")
        Alert.objects.create(
            message=f"Scraping task skipped: another scraping task is active",
            severity='warning',
            account_id=account_id,
            timestamp=timezone.now()
        )
        return {"status": "skipped", "reason": "Another scraping task is active"}

    try:
        # Acquire lock
        redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
        account = Account.objects.get(id=account_id)
        scraper = InstagramScraper()

        # Log activity start
        alert = Alert.objects.create(
            account=account,
            severity='info',
            message=f"Started scraping {source_type}:{source_id}",
            timestamp=timezone.now()
        )

        # Perform scraping
        usernames = scraper.collect_usernames(account, source_type, source_id, amount)

        # Save scraped users
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
        account.save()

        # Update alert
        alert.message += f"\nCompleted: {len(usernames)} usernames collected"
        alert.severity = 'info'
        alert.save()

        return {"status": "success", "usernames_collected": len(usernames)}

    except Exception as e:
        logging.error(f"Scrape task failed: {str(e)}", exc_info=True)
        alert = Alert.objects.filter(message__contains=f"Started scraping {source_type}:{source_id}").first()
        if alert:
            alert.message += f"\nFailed: {str(e)}"
            alert.severity = 'error'
            alert.save()
        account = Account.objects.get(id=account_id)
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

@shared_task(rate_limit='200/h')
def     enrich_user_details_task():
    """Fetch detailed metadata for users in batches using any idle account"""
    logger = logging.getLogger(__name__)
    logger.info("Starting enrich_user_details_task")
    try:
        # Select idle, warmed-up accounts
        accounts_qs = Account.objects.filter(
            status='idle',
            warmed_up=True,
            health_score__gte=50
        )

        count = accounts_qs.count()

        if count <= 1:
            accounts = accounts_qs  # use all if only 1
        elif count == 2:
            accounts = accounts_qs[:1]
        elif count == 3:
            accounts = accounts_qs[:2]
        else:
            accounts = accounts_qs[: count // 2]

        logger.info(f"Found {accounts.count()} idle accounts for enrichment")
        if not accounts:
            logger.warning("No idle, warmed-up accounts available for enrichment")
            return 0

        updated_count = 0
        skipped_count = 0
        batch_size = 50

        for account in accounts:
            if not account.can_scrape():
                logger.info(f"Account {account.username} cannot enrich users (health: {account.health_score}%)")
                continue

            cl = setup_client(account)
            if not cl:
                logger.error(f"Failed to setup client for {account.username}")
                account.update_health_score(False, "scrape")
                account.save()
                continue

            # Fetch users for this account, apply filters before slicing
            user_batch = ScrapedUser.objects.filter(
                account=account,
                details_fetched=False,
                failure_reason__isnull=True
            )[:batch_size]
            logger.info(f"Processing {user_batch.count()} users for account {account.username}")
            if not user_batch:
                logger.info(f"No users to enrich for account {account.username}")
                continue


            try:
                account.status = "enriching_users"
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
                            logger.warning(f"No data for user {user.username}")
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
                        logger.info(f"Enriched user {user.username} with account {account.username}")

                        time.sleep(random.uniform(25, 30))
                        if updated_count % 10 == 0:
                            time.sleep(random.uniform(120, 300))

                    except RateLimitError:
                        logger.warning(f"Rate limit hit for {account.username} during enrichment")
                        account.status = 'rate_limited'
                        account.save()
                        send_alert(f"Rate limit hit for {account.username} during enrichment", "warning", account)
                        break
                    except ClientError as e:
                        logger.error(f"Failed to enrich user {user.username}: {e}")
                        user.failure_reason = str(e)
                        user.details_fetched = True
                        user.save()
                        skipped_count += 1
                        if 'challenge_required' in str(e).lower():
                            account.status = 'error'
                            account.login_failures += 1
                            account.last_login_failure = timezone.now()
                            account.save()
                            send_alert(f"Challenge required for {account.username}", "error", account)
                            break
                    except Exception as e:
                        logger.error(f"Unexpected error enriching user {user.username}: {e}", exc_info=True)
                        user.failure_reason = str(e)
                        user.details_fetched = True
                        user.save()
                        skipped_count += 1
                        continue

                account.last_active = timezone.now()
                account.status = "idle"
                account.update_health_score(True, "scrape")
                account.save()
                time.sleep(random.uniform(60, 120))

            except Exception as e:
                account.status = "idle"
                account.save()
                logger.error(f"Enrichment failed for account {account.username}: {e}", exc_info=True)
                account.status = "error"
                account.update_health_score(False, "scrape")
                account.save()
                send_alert(f"Enrichment failed for {account.username}: {str(e)}", "error", account)
                continue

        logger.info(f"Enriched {updated_count} users, skipped {skipped_count}")

        # Trigger classification if users were enriched
        if updated_count > 0:
            logger.info("Queuing classify_users_task")
            classify_users_task.delay()

        # Schedule another batch if more users remain
        if ScrapedUser.objects.filter(details_fetched=False, failure_reason__isnull=True).exists():
            logger.info("Queuing another enrich_user_details_task")
            enrich_user_details_task.delay()

        return updated_count
    except Exception as e:
        logger.error(f"enrich_user_details_task failed: {e}", exc_info=True)
        raise

@shared_task(rate_limit='50/h')  # Limit to 50 calls per hour to avoid API overuse
def classify_users_task(batch_size=50):
    """Classify users for profession and country in batches"""
    logger = logging.getLogger(__name__)
    try:
        user_filter = UserFilter()
        classified_count = user_filter.classify_users_ai(batch_size=batch_size)
        logger.info(f"Classified {classified_count} users in batch")

        # Schedule another batch if more users need classification
        if ScrapedUser.objects.filter(
                Q(profession='') | Q(country=''),
                biography__isnull=False
        ).exists():
            classify_users_task.delay(batch_size=batch_size)
    except Exception as e:
        logger.error(f"Classification task failed: {e}")

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

