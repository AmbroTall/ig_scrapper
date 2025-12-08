import os
import uuid
from datetime import timedelta
import logging
import pandas as pd
import redis
import requests
from celery import shared_task, current_app
from celery.result import AsyncResult
from celery.signals import task_success, task_failure, task_revoked  # Add signal imports
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
import random
import time

from instagrapi.exceptions import RateLimitError, ClientError

from .filter import UserFilter
from .models import ScrapedUser, Account, DMCampaign, Alert, DMTemplate, DMCsvUpload, DMLog, ScheduledTask, \
    DailyMetric  # Add ScheduledTask
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .utils import setup_client, send_alert

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)


def contains_error_patterns(message):
    """Check if message contains error-related patterns (from secondary script)"""
    if not message or not isinstance(message, str):
        return True, "empty_or_invalid_message"

    message_lower = message.lower()
    error_patterns = [
        'error', 'exception', 'failed', 'failure', 'fail',
        'invalid', 'unable', 'could not', 'cannot', 'can\'t',
        'unauthorized', 'forbidden', 'denied', 'rejected',
        'rate limit', 'too many', 'try again later', 'please wait',
        'temporarily blocked', 'spam', 'suspended', 'restricted',
        'traceback', 'stack trace', 'debug', 'warning',
        'critical', 'fatal', 'abort', 'crash', 'timeout',
        '404', '403', '401', '500', '429', 'http error',
        'connection error', 'network error', 'server error',
        'api error', 'request failed', 'response error',
        'database error', 'mysql', 'sql error', 'query failed',
        'no data', 'not found', 'missing', 'null', 'undefined',
        'instagram error', 'ig error', 'checkpoint', 'challenge required',
        'verification required', 'account suspended', 'action blocked'
    ]

    for pattern in error_patterns:
        if pattern in message_lower:
            return True, f"contains_{pattern.replace(' ', '_')}"

    if any(char in message for char in ['<', '>', '{', '}', 'SELECT', 'INSERT', 'UPDATE', 'DELETE']):
        return True, "suspicious_technical_content"

    if len(message.strip()) < 5:
        return True, "message_too_short"

    return False, None

@shared_task(bind=True, ignore_result=False)
def scrape_users_task(self, account_id, source_type, source_id, amount=None):
    lock_key = "scraping_lock"
    try:
        # ✅ CHECK DAILY LIMIT FIRST
        today_metric, _ = DailyMetric.objects.get_or_create(date=timezone.now().date())
        if today_metric.scraped_count >= today_metric.scraping_threshold:
            today_metric.scraping_limit_reached = True
            today_metric.save()

            logging.warning(
                f"Daily scraping limit reached: {today_metric.scraped_count}/{today_metric.scraping_threshold}")
            send_alert(
                message=f"⛔ Daily scraping limit reached: {today_metric.scraped_count}/{today_metric.scraping_threshold}. Scraping stopped.",
                severity="warning"
            )

            # Update ScheduledTask to cancelled
            ScheduledTask.objects.filter(task_id=self.request.id).update(status='cancelled')
            return {"status": "cancelled", "reason": "daily_limit_reached"}

        # Update ScheduledTask status to 'running'
        ScheduledTask.objects.filter(task_id=self.request.id).update(status='running')

        # Fetch account within transaction to ensure consistency
        with transaction.atomic():
            account = Account.objects.select_for_update().get(id=account_id)

        # Check for existing lock
        if redis_client.get(lock_key):
            logging.warning("Scraping task already running, terminating to start new task...")
            cache_key = f"alert_scrape_task_conflict_{account_id}"
            if not cache.get(cache_key):
                send_alert(
                    message="Scraping task scheduled: New scraping task is now active.",
                    severity="warning",
                    account=account
                )
                cache.set(cache_key, True, timeout=60)
            redis_client.delete(lock_key)

        try:
            # Acquire lock and update account status
            with transaction.atomic():
                redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
                account.status = "scraping"
                account.task_id = self.request.id  # Store task_id
                account.save()

            logging.info(f"Started scraping {source_type}:{source_id} with account {account.username}")
            cache_key = f"alert_scrape_start_{source_type}_{source_id}_{account_id}"
            if not cache.get(cache_key):
                send_alert(
                    message=f"Started scraping {source_type}:{source_id} with account {account.username}",
                    severity="info",
                    account=account
                )
                cache.set(cache_key, True, timeout=60)

            scraper = InstagramScraper()
            usernames = scraper.collect_usernames(account, source_type, source_id, amount)

            # Update account after successful scrape
            with transaction.atomic():
                account.last_active = timezone.now()
                account.actions_this_hour += 1
                account.update_health_score(action_success=True, action_type="scrape")
                account.status = "idle"
                account.task_id = None  # Clear task_id
                account.save()

            logging.info(f"Completed: {len(usernames)} usernames collected with account {account.username}")
            cache_key = f"alert_scrape_complete_{source_type}_{source_id}_{account_id}"
            if not cache.get(cache_key):
                send_alert(
                    message=f"Completed: {len(usernames)} usernames collected with account {account.username}",
                    severity="info",
                    account=account
                )
                cache.set(cache_key, True, timeout=60)

            return {"status": "success", "usernames_collected": len(usernames)}

        except Exception as e:
            logging.error(f"Scrape task failed for account {account_id}: {str(e)}", exc_info=True)
            with transaction.atomic():
                account = Account.objects.select_for_update().get(id=account_id)
                account.status = "idle"
                account.task_id = None  # Clear task_id
                account.update_health_score(action_success=False, action_type="scrape")
                account.save()
            cache_key = f"alert_scrape_fail_{source_type}_{source_id}_{account_id}"
            if not cache.get(cache_key):
                send_alert(
                    message=f"Scrape task failed for account {account.username}: {str(e)}",
                    severity="error",
                    account=account
                )
                cache.set(cache_key, True, timeout=60)
            # Update ScheduledTask status to 'failed'
            ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
            raise

    except Exception as e:
        logging.error(f"Failed to initiate scrape task for account {account_id}: {e}")
        cache_key = f"alert_scrape_init_fail_{account_id}"
        if not cache.get(cache_key):
            send_alert(
                message=f"Failed to initiate scrape task for account {account_id}: {e}",
                severity="error",
                account=None
            )
            cache.set(cache_key, True, timeout=60)
        # Update ScheduledTask status to 'failed'
        ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
        raise
    finally:
        redis_client.delete(lock_key)

# Signal handlers for scrape_users_task
@task_success.connect(sender=scrape_users_task)
def scrape_success_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='completed')

@task_failure.connect(sender=scrape_users_task)
def scrape_failure_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='failed')


@shared_task(bind=True, rate_limit="500/h", ignore_result=False)
def send_dms_task(
        self,
        campaign_id=None,
        csv_upload_id=None,
        account_id=None,
        mode='campaign',
        max_dms_per_account=15
):
    """
    Send DMs for ONE account with strict limit enforcement.
    """
    dm_sender = DMSender()
    account = None
    cl = None

    try:
        # === UPDATE TASK STATUS ===
        ScheduledTask.objects.filter(task_id=self.request.id).update(status='running')

        # === 1. FETCH AND LOCK ACCOUNT ===
        try:
            with transaction.atomic():
                account = Account.objects.select_for_update().get(
                    id=account_id,
                    status='idle',
                    warmed_up=True
                )

                # ============= EARLY LIMIT CHECK =============
                account.reset_daily_counters()  # Ensure fresh counters

                if not account.can_send_dm():
                    logging.warning(
                        f"Account {account.username} cannot send DMs. "
                        f"DMs today: {account.dms_sent_today}/{account.daily_dm_limit}, "
                        f"Health: {account.health_score}%, Warmed up: {account.warmed_up}"
                    )
                    send_alert(
                        f"Account {account.username} cannot send DMs - limit reached or health too low",
                        "warning",
                        account
                    )
                    ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
                    return {"status": "failed", "reason": "account_cannot_send_dms"}

                remaining_capacity = account.daily_dm_limit - account.dms_sent_today
                if remaining_capacity <= 0:
                    logging.info(f"Account {account.username} has reached DM limit")
                    send_alert(
                        f"Account {account.username} has reached daily DM limit ({account.daily_dm_limit})",
                        "info",
                        account
                    )
                    ScheduledTask.objects.filter(task_id=self.request.id).update(status='completed')
                    return {"status": "completed", "reason": "dm_limit_reached"}

                # Adjust max_dms_per_account to respect the limit
                max_dms_per_account = min(max_dms_per_account, remaining_capacity)
                # ============================================

        except Account.DoesNotExist:
            logging.warning(f"Account {account_id} is not idle or not warmed up. Skipping task.")
            ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
            return {"status": "failed", "reason": "account_unavailable"}

        # === 2. MARK ACCOUNT AS BUSY ===
        with transaction.atomic():
            account.status = "sending_dms"
            account.task_id = self.request.id
            account.save()

        # === 3. SETUP INSTAGRAM CLIENT ===
        cl = setup_client(account)
        if not cl:
            with transaction.atomic():
                account.status = "idle"
                account.task_id = None
                account.update_health_score(action_success=False, action_type="dm")
                account.save()
            send_alert(f"Failed to setup client for {account.username}", "error", account)
            ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
            return {"status": "failed", "reason": "client_setup_failed"}

        # === 4. PREPARE USERS ===
        user_batch = []

        if mode == 'campaign':
            # --- Campaign Mode ---
            try:
                campaign = DMCampaign.objects.get(id=campaign_id)
            except DMCampaign.DoesNotExist:
                logging.error(f"Campaign {campaign_id} not found")
                send_alert(f"Campaign {campaign_id} not found", "error")
                return _finalize_account_task(self, account, cl, dm_sender, mode)

            if not campaign.is_active:
                send_alert(f"Campaign {campaign.name} is inactive", "info")
                return _finalize_account_task(self, account, cl, dm_sender, mode, campaign_id=campaign_id)

            template = campaign.template.template
            user_filter = UserFilter()
            users = user_filter.filter_users(
                professions=campaign.target_filters.get('professions', []),
                countries=campaign.target_filters.get('countries', []),
                keywords=campaign.target_filters.get('keywords', []),
                activity_days=30
            ).filter(dm_sent=False, is_active=True, is_private=False)

            if not users.exists():
                fallback_users = ScrapedUser.objects.filter(
                    # dm_sent=False,
                    is_active=True,
                    details_fetched=True,
                    is_private=False
                )
                if not fallback_users.exists():
                    send_alert(f"No eligible users for campaign {campaign.name}", "info")
                    with transaction.atomic():
                        campaign.is_active = False
                        campaign.save()
                    return _finalize_account_task(self, account, cl, dm_sender, mode, campaign_id=campaign_id)
                user_batch = list(fallback_users[:max_dms_per_account])
            else:
                user_batch = list(users[:max_dms_per_account])

        else:
            # --- CSV Mode ---
            try:
                with transaction.atomic():
                    csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
            except DMCsvUpload.DoesNotExist:
                logging.error(f"CSV upload {csv_upload_id} not found")
                send_alert(f"CSV upload {csv_upload_id} not found", "error")
                ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
                return {"status": "failed", "reason": "csv_upload_missing"}

            if csv_upload.processed:
                send_alert(f"CSV {csv_upload.name} already processed", "info")
                return _finalize_account_task(self, account, cl, dm_sender, mode, csv_upload_id=csv_upload_id)

            csv_path = os.path.join(settings.MEDIA_ROOT, csv_upload.csv_file.name)
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                logging.error(f"Failed to read CSV {csv_path}: {e}")
                send_alert(f"Failed to read CSV: {e}", "error")
                return _finalize_account_task(self, account, cl, dm_sender, mode, csv_upload_id=csv_upload_id)

            df = df[df['title'] != "Instagram User"]
            df = df[df['title'] != "Instagram user"]

            processed_usernames = set(
                DMLog.objects.filter(csv_upload=csv_upload)
                .values_list('recipient_user__username', flat=True)
            )
            df = df[~df['username'].isin(processed_usernames)]
            user_batch = df[['username', 'message']].to_dict('records')[:max_dms_per_account]

            if not user_batch:
                with transaction.atomic():
                    csv_upload.processed = True
                    csv_upload.save()
                send_alert(f"No new users in CSV {csv_upload.name}", "info")
                return _finalize_account_task(self, account, cl, dm_sender, mode, csv_upload_id=csv_upload_id)

        # === 5. SEND DMS WITH LIMIT ENFORCEMENT ===
        dm_count = 0
        successful = 0
        failed = 0
        skipped = 0
        consecutive_failures = 0

        for user in user_batch:
            # ============= CHECK LIMIT BEFORE EACH DM =============
            with transaction.atomic():
                account.refresh_from_db()

                if account.dms_sent_today >= account.daily_dm_limit:
                    logging.info(
                        f"Account {account.username} reached DM limit during sending: "
                        f"{account.dms_sent_today}/{account.daily_dm_limit}"
                    )
                    send_alert(
                        f"Account {account.username} reached daily DM limit ({account.daily_dm_limit})",
                        "warning",
                        account
                    )
                    break  # Stop sending DMs

                if not account.can_send_dm():
                    logging.warning(
                        f"Account {account.username} can no longer send DMs (health: {account.health_score}%)"
                    )
                    send_alert(
                        f"Account {account.username} stopped sending - health too low ({account.health_score}%)",
                        "warning",
                        account
                    )
                    break
            # =====================================================

            username = user.username if mode == 'campaign' else user['username']
            custom_message = user.get('message') if mode == 'csv' else None
            alert = None

            # Skip if message has error pattern
            if custom_message and contains_error_patterns(custom_message)[0]:
                reason = contains_error_patterns(custom_message)[1]
                dm_sender.logger.info(f"Skipping {username} - Message error: {reason}")
                send_alert(f"Skipping {username} - Message error: {reason}", "info", account)
                skipped += 1
                if mode == 'csv':
                    with transaction.atomic():
                        csv_upload.total_skipped += 1
                        csv_upload.save()
                continue

            try:
                with transaction.atomic():
                    alert = Alert.objects.create(
                        account=account,
                        severity="info",
                        message=f"Sending DM to {username}",
                        timestamp=timezone.now()
                    )

                    user_obj, _ = ScrapedUser.objects.get_or_create(
                        username=username,
                        defaults={
                            'account': account,
                            'source_type': 'csv' if mode == 'csv' else 'campaign',
                            'source_value': csv_upload.name if mode == 'csv' else campaign.name
                        }
                    )

                    user_id = cl.user_id_from_username(username)
                    dm_text = custom_message or dm_sender.generate_dynamic_dm(user_obj.biography, template)
                    cl.direct_send(dm_text, [user_id])

                    # Success updates
                    user_obj.dm_sent = True
                    user_obj.dm_sent_at = timezone.now()
                    user_obj.dm_account = account
                    user_obj.save()

                    DMLog.objects.create(
                        sender_account=account,
                        recipient_user=user_obj,
                        message=dm_text,
                        sent_at=timezone.now(),
                        csv_upload=csv_upload if mode == 'csv' else None,
                        campaign=campaign if mode == 'campaign' else None
                    )

                    if mode == 'campaign':
                        campaign.total_sent += 1
                        campaign.save()
                    else:
                        csv_upload.total_successful += 1
                        csv_upload.total_processed += 1
                        csv_upload.save()

                    account.dms_sent_today += 1
                    account.last_active = timezone.now()
                    account.update_health_score(action_success=True, action_type="dm")
                    account.save()

                    alert.message += " - Success"
                    alert.save()

                dm_sender.logger.info(
                    f"Sent DM to {username} from {account.username} "
                    f"({account.dms_sent_today}/{account.daily_dm_limit})"
                )
                send_alert(f"Sent DM to {username}", "info", account)
                successful += 1
                dm_count += 1
                consecutive_failures = 0

                if dm_count % 3 == 0:
                    dm_sender.perform_activity(cl, account, campaign if mode == 'campaign' else None)

                time.sleep(random.uniform(40, 120))

            except RateLimitError:
                with transaction.atomic():
                    account.status = "rate_limited"
                    account.task_id = None
                    account.save()
                    if alert:
                        alert.message += " - Rate limit"
                        alert.severity = "warning"
                        alert.save()
                send_alert(f"Rate limit hit on {account.username}", "warning", account)
                failed += 1
                break

            except ClientError as e:
                error_str = str(e).lower()
                with transaction.atomic():
                    if "403" in error_str:
                        user_obj.is_active = False
                        user_obj.failure_reason = "403: DM restricted"
                        user_obj.save()
                        skipped += 1
                        alert.message += " - 403, skipped"
                        alert.severity = "info"
                        alert.save()
                        send_alert(f"403: Cannot DM {username}", "info", account)
                        if mode == 'csv':
                            csv_upload.total_skipped += 1
                            csv_upload.total_processed += 1
                            csv_upload.save()
                    else:
                        account.update_health_score(action_success=False, action_type="dm")
                        account.save()
                        failed += 1
                        alert.message += f" - Error: {e}"
                        alert.severity = "error"
                        alert.save()
                        send_alert(f"Failed DM to {username}: {e}", "error", account)
                        if mode == 'csv':
                            csv_upload.total_failed += 1
                            csv_upload.total_processed += 1
                            csv_upload.save()
                    consecutive_failures += 1

            except Exception as e:
                dm_sender.logger.error(f"Unexpected error sending DM to {username}: {e}")
                send_alert(f"DM error: {e}", "error", account)
                failed += 1
                consecutive_failures += 1
                if alert:
                    alert.message += f" - {e}"
                    alert.severity = "error"
                    alert.save()

            if consecutive_failures >= 5:
                send_alert(f"5 consecutive failures on {account.username}, stopping", "error", account)
                break

        # === 6. FINALIZE ===
        return _finalize_account_task(
            self, account, cl, dm_sender,
            mode, campaign_id, csv_upload_id,
            successful, failed, skipped
        )

    except Exception as e:
        logging.error(f"DM task failed: {e}", exc_info=True)
        send_alert(f"DM task failed: {str(e)}", "error")

        if account:
            try:
                with transaction.atomic():
                    acct = Account.objects.select_for_update().get(id=account.id)
                    acct.status = "idle"
                    acct.task_id = None
                    acct.save()
            except Exception as reset_err:
                logging.error(f"Failed to reset account {account.id}: {reset_err}")

        ScheduledTask.objects.filter(task_id=self.request.id).update(status='failed')
        raise


def _finalize_account_task(
    task, account, cl, dm_sender,
    mode, campaign_id=None, csv_upload_id=None,
    successful=0, failed=0, skipped=0
):
    """Safely reset account and update campaign/CSV status"""
    try:
        with transaction.atomic():
            acct = Account.objects.select_for_update().get(id=account.id)
            acct.status = "idle"
            acct.task_id = None
            acct.save()
    except Exception as e:
        logging.error(f"Failed to finalize account {account.id}: {e}")

    total = successful + failed + skipped
    msg = f"DM task complete: {account.username} → {successful}s, {failed}f, {skipped}sk"
    dm_sender.logger.info(msg)
    send_alert(msg, "info", account)

    # === Campaign Completion ===
    if mode == 'campaign' and campaign_id:
        try:
            campaign = DMCampaign.objects.get(id=campaign_id)
            user_filter = UserFilter()
            remaining = user_filter.filter_users(
                professions=campaign.target_filters.get('professions', []),
                countries=campaign.target_filters.get('countries', []),
                keywords=campaign.target_filters.get('keywords', []),
                activity_days=30
            ).filter(dm_sent=False, is_active=True, is_private=False).exists()

            if not remaining:
                fallback = ScrapedUser.objects.filter(dm_sent=False, is_active=True).exists()
                if not fallback:
                    with transaction.atomic():
                        campaign.is_active = False
                        campaign.save()
                    send_alert(f"Campaign {campaign.name} completed", "info")
        except DMCampaign.DoesNotExist:
            pass

    # === CSV Completion ===
    elif mode == 'csv' and csv_upload_id:
        try:
            with transaction.atomic():
                csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                total_logs = DMLog.objects.filter(csv_upload=csv_upload).count()
                df = pd.read_csv(os.path.join(settings.MEDIA_ROOT, csv_upload.csv_file.name))
                if total_logs >= len(df):
                    csv_upload.processed = True
                    csv_upload.save()
                    send_alert(f"CSV {csv_upload.name} fully processed", "info")
        except Exception as e:
            send_alert(f"CSV completion check failed: {e}", "error")

    ScheduledTask.objects.filter(task_id=task.request.id).update(status='success')
    return {
        "status": "success",
        "account": account.username,
        "sent": successful,
        "failed": failed,
        "skipped": skipped
    }

# Signal handlers for send_dms_task
@task_success.connect(sender=send_dms_task)
def dm_success_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='completed')

@task_failure.connect(sender=send_dms_task)
def dm_failure_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='failed')


# ──────────────────────────────────────────────────────────────
# Instagram Web API Headers + Proxy Config
# ──────────────────────────────────────────────────────────────
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'X-IG-App-ID': '936619743392459',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': '*/*',
    'Referer': 'https://www.instagram.com/',
    'Origin': 'https://www.instagram.com',
    'Accept-Language': 'en-US,en;q=0.9',
}

PROXY_HOST = "core-residential.evomi.com"
PROXY_PORT = "1000"
PROXY_USER_BASE = "robertthom3"
PROXY_PASSWORD = "3DDQ6eQd4OK8Xx2EZQYW"


def get_fresh_proxy_session():
    session_id = str(uuid.uuid4())[:8]
    proxy_user = f"{PROXY_USER_BASE}_session_{session_id}"
    proxy_url = f"http://{PROXY_USER_BASE}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
    # proxy_url = f"http://core-residential.evomi.com:1000:robertthom3:3DDQ6eQd4OK8Xx2EZQYW"
    return {"http": proxy_url, "https": proxy_url}

@shared_task(bind=True, max_retries=10, default_retry_delay=60, rate_limit="2000/h")
def enrich_user_details_task(self):
    lock_key = "enrichment_proxy_lock_v3"

    # Allow max 10 parallel enrichment tasks
    current = redis_client.get(lock_key)
    if current and int(current) >= 10:
        send_alert("Max parallel enrichment tasks reached (10) – task requeued", "warning")
        raise self.retry(countdown=120)

    try:
        redis_client.incr(lock_key)
        redis_client.expire(lock_key, 3600)

        send_alert("Proxy-based enrichment task started (no account login)", "info")

        batch_size = 70
        users = ScrapedUser.objects.filter(
            details_fetched=False,
            failure_reason__isnull=True
        )[:batch_size]

        if not users:
            send_alert("No more users to enrich – enrichment job completed", "success")
            return 0

        proxies = get_fresh_proxy_session()
        updated = 0
        failed = 0

        for user in users:
            try:
                url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={user.username}"

                resp = requests.get(
                    url,
                    headers=HEADERS,
                    proxies=proxies,
                    timeout=20,
                    verify=False
                )

                if resp.status_code == 404:
                    user.failure_reason = "Account not found / deleted"
                    user.details_fetched = True
                    user.save(update_fields=['failure_reason', 'details_fetched'])
                    send_alert(f"@{user.username} → not found (404)", "warning")
                    failed += 1
                    continue

                if resp.status_code != 200:
                    send_alert(f"HTTP {resp.status_code} for @{user.username} – retrying later", "warning")
                    raise Exception("Bad response")

                data = resp.json()
                ig_user = data.get("data", {}).get("user")
                if not ig_user:
                    user.failure_reason = "Empty response"
                    user.details_fetched = True
                    user.save(update_fields=['failure_reason', 'details_fetched'])
                    failed += 1
                    continue

                # Extract data safely
                bio = (ig_user.get("biography") or "").encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')

                user.user_id = ig_user.get("id")
                user.full_name = ig_user.get("full_name", "")[:150]
                user.biography = bio
                user.follower_count = ig_user.get("edge_followed_by", {}).get("count")
                user.following_count = ig_user.get("edge_follow", {}).get("count")
                user.post_count = ig_user.get("edge_owner_to_timeline_media", {}).get("count")
                user.is_private = ig_user.get("is_private", False)
                user.is_business_account = ig_user.get("is_business_account", False)
                user.is_verified = ig_user.get("is_verified", False)
                user.profile_pic_url = ig_user.get("profile_pic_url_hd") or ig_user.get("profile_pic_url")
                user.external_url = ig_user.get("external_url", "")
                user.details_fetched = True
                user.enriched_at = timezone.now()
                user.save()

                updated += 1
                send_alert(f"ENRICHED @{user.username} → {user.follower_count} followers", "success")

                time.sleep(random.uniform(0.9, 2.3))

            except Exception as e:
                # Don't mark failed — next task will retry with fresh IP
                send_alert(f"Failed @{user.username}: {str(e)} – will retry later", "warning")
                failed += 1
                continue

        # Summary alert
        send_alert(f"Enrichment batch completed → {updated} enriched, {failed} skipped/failed", "info")

        # Requeue if more work
        if ScrapedUser.objects.filter(details_fetched=False, failure_reason__isnull=True).exists():
            send_alert("More users pending → requeuing enrichment task", "info")
            enrich_user_details_task.delay()

        return updated

    except Exception as e:
        send_alert(f"Enrichment task crashed: {str(e)}", "error")
        raise self.retry(countdown=60 * (2 ** self.request.retries))

    finally:
        redis_client.decr(lock_key)
        send_alert("Enrichment task finished & lock released", "info")

# Signal handlers for enrich_user_details_task
@task_success.connect(sender=enrich_user_details_task)
def enrich_success_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='completed')

@task_failure.connect(sender=enrich_user_details_task)
def enrich_failure_handler(sender, **kwargs):
    ScheduledTask.objects.filter(task_id=sender.request.id).update(status='failed')

# The following tasks remain unchanged as they are not mentioned in the scheduling requirements
@shared_task(rate_limit='10/h')
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
        # Update ScheduledTask status to 'cancelled'
        ScheduledTask.objects.filter(task_id=task_id).update(status='cancelled')
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

        oldest_ids = list(
            Alert.objects.order_by('timestamp')
            .values_list('id', flat=True)[:excess_count]
        )

        deleted_count, _ = Alert.objects.filter(id__in=oldest_ids).delete()

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
    """Check accounts with scraping/sending_dms status and reset to idle if task_id is not in Celery queue."""
    lock_key = "reset_stale_accounts_lock"
    if redis_client.get(lock_key):
        logging.warning("Reset stale accounts task already running, skipping")
        send_alert("Reset stale accounts task skipped: another instance is active", "warning")
        return 0

    try:
        redis_client.set(lock_key, self.request.id, ex=3600)
        logging.info("Starting reset_stale_accounts_task")
        send_alert("Starting reset_stale_accounts_task", "info")

        # Define statuses that should be reset
        resettable_statuses = ["scraping", "sending_dms"]

        # Get accounts with resettable statuses only
        non_idle_accounts = Account.objects.filter(status__in=resettable_statuses)
        if not non_idle_accounts:
            logging.info("No accounts with resettable statuses found")
            send_alert("No accounts with resettable statuses found", "info")
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
                logging.info(
                    f"Resetting account {account.username}: status={account.status}, task_id={account.task_id}")
                send_alert(f"Resetting account {account.username}: status={account.status}, task_id={account.task_id}",
                           "info", account)
                account.status = "idle"
                account.task_id = None
                account.save()
                reset_count += 1
            elif not account.task_id:
                logging.info(f"Resetting account {account.username}: status={account.status}, no task_id")
                send_alert(f"Resetting account {account.username}: status={account.status}, no task_id", "info",
                           account)
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

@task_revoked.connect
def on_task_revoked(sender=None, request=None, terminated=None, signum=None, expired=None, **kwargs):
    """
    This runs **every time any Celery task is revoked** (revoke(), kill, terminate, etc.).
    """
    task_id = request.id if request else kwargs.get('task_id')
    if not task_id:
        return

    try:
        task = ScheduledTask.objects.get(task_id=task_id)
        if task.status in ('scheduled', 'running'):
            task.status = 'cancelled'
            task.save(update_fields=['status'])

            message = (
                f"ScheduledTask {task.id} (task_id={task_id}) marked as cancelled "
                f"by Celery revoke signal."
            )
            logging.info(message)
            send_alert(message, "info")
    except ScheduledTask.DoesNotExist:
        # The task was never stored in ScheduledTask (e.g. ad-hoc tasks) – ignore.
        pass
    except Exception as e:
        x = f"Failed to update ScheduledTask on revoke {task_id}: {e}"
        send_alert(x, "error")

# Define your scraping task names (MUST match exactly!)
SCRAPING_TASK_NAMES = [
    'dmbot.tasks.scrape_users_task',
]

@shared_task(bind=True)
def enforce_daily_scraping_limit(self):
    today = timezone.now().date()
    metric, _ = DailyMetric.objects.get_or_create(date=today)

    # Update scraped count
    actual_scraped_count = ScrapedUser.objects.filter(scraped_at__date=today).count()
    metric.scraped_count = actual_scraped_count

    # Update enriched count (users whose details were fetched today)
    actual_enriched_count = ScrapedUser.objects.filter(
        scraped_at__date=today,
        details_fetched=True
    ).count()
    metric.enriched_count = actual_enriched_count

    # Update DM sent count
    actual_dm_count = DMLog.objects.filter(sent_at__date=today).count()
    metric.dm_sent_count = actual_dm_count

    # If limit reached and not already handled today
    if actual_scraped_count >= metric.scraping_threshold and not metric.scraping_limit_reached:
        metric.scraping_limit_reached = True
        metric.save()

        # Prevent spam: only send alert once per day
        cache_key = f"scraping_limit_alert_sent_{today}"
        if not cache.get(cache_key):
            send_alert(
                message=
                    f"Daily scraping threshold hit: {actual_scraped_count:,} / {metric.scraping_threshold:,} users.\n\n"
                    "All active scraping tasks have been terminated.\n"
                    "Scraping will resume automatically tomorrow."
                ,
                severity="critical",
            )
            cache.set(cache_key, True, timeout=86400)  # 24 hours

        # REVOKE ONLY SCRAPING TASKS
        inspector = current_app.control.inspect()
        active_tasks = inspector.active() or {}
        revoked_count = 0

        for worker, tasks in active_tasks.items():
            for task in tasks:
                task_name = task.get('name') or task.get('task')
                task_id = task['id']

                if task_name in SCRAPING_TASK_NAMES:
                    current_app.control.revoke(task_id, terminate=True, signal='SIGTERM')
                    revoked_count += 1

        # Log result
        if revoked_count > 0:
            print(f"[SCRAPING LIMIT] Revoked {revoked_count} active scraping task(s).")
        else:
            print("[SCRAPING LIMIT] Limit reached, but no active scraping tasks to revoke.")
    else:
        # Reset flag if count dropped below threshold (e.g. manual cleanup)
        if metric.scraping_limit_reached and actual_scraped_count < metric.scraping_threshold:
            metric.scraping_limit_reached = False
            metric.save()

    metric.save()

