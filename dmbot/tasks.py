import os
from datetime import timedelta
import logging

import pandas as pd
import redis
from celery import shared_task, current_app
from celery.result import AsyncResult
from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
import logging
import random
import time

from instagrapi.exceptions import RateLimitError, ClientError

from .filter import UserFilter
from .models import ScrapedUser, Account, DMCampaign, Alert, DMTemplate, DMCsvUpload, DMLog
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .utils import setup_client, send_alert

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)


@shared_task(bind=True, ignore_result=False)
def scrape_users_task(self, account_id, source_type, source_id, amount=None):
    lock_key = "scraping_lock"
    try:
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
    finally:
        redis_client.delete(lock_key)


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


@shared_task(bind=True, rate_limit="200/h")
def send_dms_task(self, campaign_id=None, csv_upload_id=None, mode='campaign', max_dms_per_account=15):
    dm_sender = DMSender()
    try:
        if mode == 'campaign':
            with transaction.atomic():
                campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                if not campaign.is_active:
                    dm_sender.logger.info(f"Campaign {campaign.name} is inactive")
                    send_alert(f"Campaign {campaign.name} is inactive", "info")
                    return
                if not DMTemplate.objects.filter(id=campaign.template_id, active=True).exists():
                    dm_sender.logger.error(f"No active template for campaign {campaign.name}")
                    send_alert(f"No active template for campaign {campaign.name}", "error")
                    campaign.is_active = False
                    campaign.save()
                    return
                template = campaign.template.template
                accounts = campaign.accounts.filter(status="idle", warmed_up=True)
                user_filter = UserFilter()
                users = user_filter.filter_users(
                    professions=campaign.target_filters.get('professions', []),
                    countries=campaign.target_filters.get('countries', []),
                    keywords=campaign.target_filters.get('keywords', []),
                    activity_days=30
                ).filter(
                    dm_sent=False,
                    is_active=True,
                    is_private=False
                )
                if not users.exists():
                    users = ScrapedUser.objects.filter(
                        dm_sent=False,
                        is_active=True,
                        details_fetched=True,
                        is_private=False
                    )
                    if not users.exists():
                        dm_sender.logger.error(f"No eligible users available for campaign {campaign.name}")
                        send_alert(f"No eligible users available for campaign {campaign.name}", "error")
                        campaign.is_active = False
                        campaign.save()
                        return
        else:  # CSV mode
            with transaction.atomic():
                csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                if csv_upload.processed:
                    dm_sender.logger.info(f"CSV campaign {csv_upload.name} already processed")
                    send_alert(f"CSV campaign {csv_upload.name} already processed", "info")
                    return
                accounts = csv_upload.accounts.filter(status="idle", warmed_up=True)
                csv_path = os.path.join(settings.MEDIA_ROOT, csv_upload.csv_file.name)
                df = pd.read_csv(csv_path)
                df = df[df['title'] != "Instagram User"]
                df = df[df['title'] != "Instagram user"]
                processed_usernames = set(
                    DMLog.objects.filter(csv_upload=csv_upload).values_list('recipient_user__username', flat=True))
                df = df[~df['username'].isin(processed_usernames)]
                users = df[['username', 'message']].to_dict('records')
                if not users:
                    dm_sender.logger.info(f"No new users to process in CSV {csv_upload.name}")
                    send_alert(f"No new users to process in CSV {csv_upload.name}", "info")
                    csv_upload.processed = True
                    csv_upload.save()
                    return

        for account in accounts:
            if not account.can_send_dm():
                dm_sender.logger.info(f"Account {account.username} cannot send DMs")
                send_alert(f"Account {account.username} cannot send DMs", "info", account)
                continue

            cl = setup_client(account)
            if not cl:
                with transaction.atomic():
                    account = Account.objects.select_for_update().get(id=account.id)
                    account.update_health_score(False, "dm")
                    account.save()
                send_alert(f"Failed to setup client for {account.username}", "error", account)
                continue

            with transaction.atomic():
                account = Account.objects.select_for_update().get(id=account.id)
                account.status = "sending_dms"
                account.save()

            dm_count = 0
            consecutive_failures = 0
            successful = 0
            failed = 0
            skipped = 0

            user_batch = users[:max_dms_per_account] if mode == 'campaign' else users
            for user in user_batch:
                try:
                    username = user.username if mode == 'campaign' else user['username']
                    custom_message = user['message'] if mode == 'csv' else None
                    has_error, error_reason = contains_error_patterns(custom_message) if custom_message else (False,
                                                                                                              None)
                    if has_error:
                        dm_sender.logger.info(f"Skipping {username} - Message contains error pattern: {error_reason}")
                        send_alert(f"Skipping {username} - Message contains error pattern: {error_reason}", "info")
                        skipped += 1
                        if mode == 'csv':
                            with transaction.atomic():
                                csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                                csv_upload.total_skipped += 1
                                csv_upload.save()
                        continue

                    with transaction.atomic():
                        alert = Alert.objects.create(
                            account=account,
                            severity="info",
                            message=f"Sending DM to {username} for {'campaign' if mode == 'campaign' else 'CSV'}",
                            timestamp=timezone.now()
                        )
                        user_obj, _ = ScrapedUser.objects.get_or_create(username=username)
                        user_id = cl.user_id_from_username(username)
                        dm_text = custom_message if custom_message else dm_sender.generate_dynamic_dm(
                            user_obj.biography, template)
                        cl.direct_send(dm_text, [user_id])
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
                            campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                            campaign.total_sent += 1
                            campaign.save()
                        else:
                            csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                            csv_upload.total_successful += 1
                            csv_upload.total_processed += 1
                            csv_upload.save()
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.dms_sent_today += 1
                        account.last_active = timezone.now()
                        account.update_health_score(True, "dm")
                        account.save()
                        alert.message += "\nDM sent successfully"
                        alert.save()

                    dm_sender.logger.info(f"Sent DM to {username} from {account.username}")
                    send_alert(f"Sent DM to {username} from {account.username}", "info", account)
                    dm_count += 1
                    successful += 1
                    consecutive_failures = 0
                    if dm_count % 3 == 0:
                        dm_sender.perform_activity(cl, account, campaign if mode == 'campaign' else None)
                    time.sleep(random.uniform(40, 120))

                except RateLimitError:
                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.status = "rate_limited"
                        account.save()
                        alert = Alert.objects.select_for_update().get(id=alert.id)
                        alert.message += "\nFailed: Rate limit hit"
                        alert.severity = "warning"
                        alert.save()
                    send_alert(f"Rate limit hit for DMs on {account.username}", "warning", account)
                    failed += 1
                    consecutive_failures += 1
                    break
                except ClientError as e:
                    error_str = str(e).lower()
                    with transaction.atomic():
                        user_obj, _ = ScrapedUser.objects.get_or_create(username=username)
                        alert = Alert.objects.select_for_update().get(id=alert.id)
                        if "403" in error_str:
                            dm_sender.logger.info(f"Cannot send DM to {username} due to 403 error, skipping")
                            send_alert(f"Cannot send DM to {username} due to 403 error, skipping", "info", account)
                            user_obj.is_active = False
                            user_obj.failure_reason = "403 Forbidden: DM restricted"
                            user_obj.save()
                            alert.message += "\nFailed: 403 Forbidden, user skipped"
                            alert.severity = "info"
                            alert.save()
                            skipped += 1
                        else:
                            dm_sender.logger.error(f"Failed to send DM to {username}: {e}")
                            send_alert(f"Failed to send DM to {username}: {e}", "error", account)
                            account.update_health_score(False, "dm")
                            account.save()
                            alert.message += f"\nFailed: {str(e)}"
                            alert.severity = "error"
                            alert.save()
                            failed += 1
                            consecutive_failures += 1
                        if mode == 'csv':
                            csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                            csv_upload.total_failed += 1 if "403" not in error_str else 0
                            csv_upload.total_skipped += 1 if "403" in error_str else 0
                            csv_upload.total_processed += 1
                            csv_upload.save()
                except Exception as e:
                    dm_sender.logger.error(f"Unexpected DM error for {username}: {e}")
                    send_alert(f"Unexpected DM error for {username}: {e}", "error", account)
                    with transaction.atomic():
                        user_obj, _ = ScrapedUser.objects.get_or_create(username=username)
                        user_obj.failure_reason = str(e)
                        user_obj.save()
                        alert = Alert.objects.select_for_update().get(id=alert.id)
                        alert.message += f"\nFailed: {str(e)}"
                        alert.severity = "error"
                        alert.save()
                        if mode == 'csv':
                            csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                            csv_upload.total_failed += 1
                            csv_upload.total_processed += 1
                            csv_upload.save()
                    failed += 1
                    consecutive_failures += 1

                if consecutive_failures >= 5:
                    dm_sender.logger.error(f"Reached 5 consecutive failures for account {account.username}")
                    send_alert(f"Reached 5 consecutive failures for account {account.username}, stopping", "error",
                               account)
                    break

            with transaction.atomic():
                account = Account.objects.select_for_update().get(id=account.id)
                account.status = "idle"
                account.save()

        if mode == 'campaign':
            with transaction.atomic():
                campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                remaining_users = user_filter.filter_users(
                    professions=campaign.target_filters.get('professions', []),
                    countries=campaign.target_filters.get('countries', []),
                    keywords=campaign.target_filters.get('keywords', []),
                    activity_days=30
                ).filter(
                    dm_sent=False,
                    is_active=True,
                    is_private=False
                )
                if not remaining_users.exists():
                    dm_sender.logger.info(f"Campaign {campaign.name} completed: no eligible users remaining")
                    send_alert(f"Campaign {campaign.name} completed: no eligible users remaining", "info")
                    campaign.is_active = False
                campaign.save()
        else:
            with transaction.atomic():
                csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                remaining_users = pd.read_csv(os.path.join(settings.MEDIA_ROOT, csv_upload.csv_file.name))
                remaining_users = remaining_users[~remaining_users['username'].isin(
                    set(DMLog.objects.filter(csv_upload=csv_upload).values_list('recipient_user__username', flat=True))
                )]
                if not len(remaining_users):
                    csv_upload.processed = True
                    dm_sender.logger.info(f"CSV campaign {csv_upload.name} completed: no users remaining")
                    send_alert(f"CSV campaign {csv_upload.name} completed: no users remaining", "info")
                csv_upload.save()

    except Exception as e:
        dm_sender.logger.error(f"{'Campaign' if mode == 'campaign' else 'CSV campaign'} failed: {e}")
        send_alert(f"{'Campaign' if mode == 'campaign' else 'CSV campaign'} failed: {str(e)}", "error")
        with transaction.atomic():
            if mode == 'campaign':
                try:
                    campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                    campaign.is_active = False
                    campaign.save()
                except DMCampaign.DoesNotExist:
                    dm_sender.logger.error(f"Campaign {campaign_id} does not exist")
                    send_alert(f"Campaign {campaign_id} does not exist", "error")
            else:
                try:
                    csv_upload = DMCsvUpload.objects.select_for_update().get(id=csv_upload_id)
                    csv_upload.processed = True
                    csv_upload.save()
                except DMCsvUpload.DoesNotExist:
                    dm_sender.logger.error(f"CSV upload {csv_upload_id} does not exist")
                    send_alert(f"CSV upload {csv_upload_id} does not exist", "error")

@shared_task(bind=True, rate_limit="200/h")
def enrich_user_details_task(self):
    """Fetch detailed metadata for users in batches using selected or any idle account"""
    lock_key = "enrichment_lock"
    selected_accounts_key = "enrichment_selected_accounts"
    try:
        if redis_client.get(lock_key):
            logging.warning("Enrichment task already running, skipping new task")
            cache_key = f"alert_enrich_conflict_{self.request.id}"
            if not cache.get(cache_key):
                send_alert("Enrichment task skipped: another enrichment task is active", "warning")
                cache.set(cache_key, True, timeout=60)
            return 0

        try:
            with transaction.atomic():
                redis_client.set(lock_key, self.request.id, ex=3600)  # Lock for 1 hour
            logging.info("Starting enrich_user_details_task")
            cache_key = f"alert_enrich_start_{self.request.id}"
            if not cache.get(cache_key):
                send_alert("Starting enrich_user_details_task", "info")
                cache.set(cache_key, True, timeout=60)

            # Use selected accounts if available
            selected_ids = redis_client.lrange(selected_accounts_key, 0, -1)
            if selected_ids:
                accounts = Account.objects.filter(
                    id__in=[int(aid) for aid in selected_ids],
                    status="idle",
                    warmed_up=True,
                    health_score__gte=50
                )
                redis_client.delete(selected_accounts_key)  # Clear after use
            else:
                accounts_qs = Account.objects.filter(
                    status="idle",
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
                    accounts = accounts_qs[:count // 2]

            logging.info(f"Found {accounts.count()} idle accounts for enrichment")
            cache_key = f"alert_enrich_accounts_{self.request.id}"
            if not cache.get(cache_key):
                send_alert(f"Found {accounts.count()} idle accounts for enrichment", "info")
                cache.set(cache_key, True, timeout=60)

            if not accounts:
                logging.warning("No idle, warmed-up accounts available for enrichment")
                cache_key = f"alert_no_accounts_{self.request.id}"
                if not cache.get(cache_key):
                    send_alert("No idle, warmed-up accounts available for enrichment", "warning")
                    cache.set(cache_key, True, timeout=60)
                return 0

            updated_count = 0
            skipped_count = 0
            batch_size = 50

            for account in accounts:
                if not account.can_scrape():
                    logging.info(f"Account {account.username} cannot enrich users (health: {account.health_score}%)")
                    cache_key = f"alert_account_cannot_enrich_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Account {account.username} cannot enrich users (health: {account.health_score}%)",
                                   "info", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                cl = setup_client(account)
                if not cl:
                    logging.error(f"Failed to setup client for {account.username}")
                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.update_health_score(False, "scrape")
                        account.save()
                    cache_key = f"alert_client_setup_fail_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Failed to setup client for {account.username}", "error", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                user_batch = ScrapedUser.objects.filter(
                    # account=account,
                    details_fetched=False,
                    failure_reason__isnull=True
                )[:batch_size]
                logging.info(f"Processing {user_batch.count()} users for account {account.username}")
                cache_key = f"alert_processing_users_{account.id}"
                if not cache.get(cache_key):
                    send_alert(f"Processing {user_batch.count()} users for account {account.username}", "info", account)
                    cache.set(cache_key, True, timeout=60)

                if not user_batch:
                    logging.info(f"No users to enrich for account {account.username}")
                    cache_key = f"alert_no_users_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"No users to enrich for account {account.username}", "info", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                try:
                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.status = "enriching_users"
                        account.task_id = self.request.id  # Store task_id
                        account.save()

                    scraper = InstagramScraper()
                    for user in user_batch:
                        try:
                            user_data = scraper._get_user_detailed_info(cl, user.username)
                            with transaction.atomic():
                                user = ScrapedUser.objects.select_for_update().get(id=user.id)
                                if not user_data:
                                    user.failure_reason = "Failed to fetch detailed info"
                                    user.is_active = False
                                    user.details_fetched = True
                                    user.save()
                                    skipped_count += 1
                                    logging.warning(f"No data for user {user.username}")
                                    cache_key = f"alert_no_data_{user.username}_{account.id}"
                                    if not cache.get(cache_key):
                                        send_alert(f"No data for user {user.username}", "warning", account)
                                        cache.set(cache_key, True, timeout=60)
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
                                cache_key = f"alert_enriched_user_{user.username}_{account.id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Enriched user {user.username} with account {account.username}", "info",
                                               account)
                                    cache.set(cache_key, True, timeout=60)

                            time.sleep(random.uniform(25, 30))
                            if updated_count % 10 == 0:
                                time.sleep(random.uniform(120, 300))

                        except RateLimitError:
                            logging.warning(f"Rate limit hit for {account.username} during enrichment")
                            with transaction.atomic():
                                account = Account.objects.select_for_update().get(id=account.id)
                                account.status = "rate_limited"
                                account.task_id = None  # Clear task_id
                                account.save()
                            cache_key = f"alert_rate_limit_enrich_{account.id}"
                            if not cache.get(cache_key):
                                send_alert(f"Rate limit hit for {account.username} during enrichment", "warning",
                                           account)
                                cache.set(cache_key, True, timeout=60)
                            break
                        except ClientError as e:
                            logging.error(f"Failed to enrich user {user.username}: {e}")
                            with transaction.atomic():
                                user = ScrapedUser.objects.select_for_update().get(id=user.id)
                                user.failure_reason = str(e)
                                user.details_fetched = True
                                user.save()
                            skipped_count += 1
                            cache_key = f"alert_enrich_error_{user.username}_{account.id}"
                            if not cache.get(cache_key):
                                send_alert(f"Failed to enrich user {user.username}: {e}", "error", account)
                                cache.set(cache_key, True, timeout=60)
                            if "challenge_required" in str(e).lower():
                                with transaction.atomic():
                                    account = Account.objects.select_for_update().get(id=account.id)
                                    account.status = "error"
                                    account.login_failures += 1
                                    account.last_login_failure = timezone.now()
                                    account.task_id = None  # Clear task_id
                                    account.save()
                                cache_key = f"alert_challenge_required_{account.id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Challenge required for {account.username}", "error", account)
                                    cache.set(cache_key, True, timeout=60)
                                break
                        except Exception as e:
                            logging.error(f"Unexpected error enriching user {user.username}: {e}", exc_info=True)
                            with transaction.atomic():
                                user = ScrapedUser.objects.select_for_update().get(id=user.id)
                                user.failure_reason = str(e)
                                user.details_fetched = True
                                user.save()
                            skipped_count += 1
                            cache_key = f"alert_unexpected_error_{user.username}_{account.id}"
                            if not cache.get(cache_key):
                                send_alert(f"Unexpected error enriching user {user.username}: {e}", "error", account)
                                cache.set(cache_key, True, timeout=60)
                            continue

                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.last_active = timezone.now()
                        account.status = "idle"
                        account.update_health_score(True, "scrape")
                        account.task_id = None  # Clear task_id
                        account.save()
                    time.sleep(random.uniform(60, 120))

                except Exception as e:
                    logging.error(f"Enrichment failed for account {account.username}: {e}", exc_info=True)
                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.status = "idle"
                        account.task_id = None  # Clear task_id
                        account.update_health_score(False, "scrape")
                        account.save()
                    cache_key = f"alert_enrich_account_fail_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Enrichment failed for account {account.username}: {e}", "error", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

            logging.info(f"Enriched {updated_count} users, skipped {skipped_count}")
            cache_key = f"alert_enrich_summary_{self.request.id}"
            if not cache.get(cache_key):
                send_alert(f"Enriched {updated_count} users, skipped {skipped_count}", "info")
                cache.set(cache_key, True, timeout=60)

            if updated_count > 0:
                logging.info("Queuing classify_users_task")
                cache_key = f"alert_queue_classify_{self.request.id}"
                if not cache.get(cache_key):
                    send_alert("Queuing classify_users_task", "info")
                    cache.set(cache_key, True, timeout=60)
                classify_users_task.delay()

            if ScrapedUser.objects.filter(details_fetched=False, failure_reason__isnull=True).exists():
                logging.info("Queuing another enrich_user_details_task")
                cache_key = f"alert_queue_enrich_{self.request.id}"
                if not cache.get(cache_key):
                    send_alert("Queuing another enrich_user_details_task", "info")
                    cache.set(cache_key, True, timeout=60)
                enrich_user_details_task.delay()

            return updated_count

        except Exception as e:
            logging.error(f"enrich_user_details_task failed: {e}", exc_info=True)
            cache_key = f"alert_enrich_task_fail_{self.request.id}"
            if not cache.get(cache_key):
                send_alert(f"enrich_user_details_task failed: {e}", "error")
                cache.set(cache_key, True, timeout=60)
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
