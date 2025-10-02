from datetime import timedelta

from celery import shared_task
from django.db.models import Q
from django.utils import timezone
import logging
import random
import time

from .filter import UserFilter
from .models import ScrapedUser, Account, DMCampaign, Alert
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .utils import setup_client, send_alert

@shared_task
def scrape_users_task(account_id, source_type, source_id, amount=None):
    try:
        account = Account.objects.get(id=account_id)
        scraper = InstagramScraper()
        delay = random.randint(10, 60)
        time.sleep(delay)
        usernames = scraper.collect_usernames(account, source_type, source_id, amount)
        if usernames:
            scraper.store_users_enhanced(usernames, account, source_type, source_id)
        # expand_users_task.delay(account.id)
        logging.info(f"Scraping task completed for {account.username}: {len(usernames)} users")
    except Exception as e:
        logging.error(f"Scraping task failed for account {account_id}: {e}")
        if account:
            account.status = "error"
            account.save()
            send_alert(f"Scraping task failed for {account.username}: {str(e)}", "error", account)

@shared_task
def send_dms_task(campaign_id, max_dms_per_account=15):
    dm_sender = DMSender()
    try:
        dm_sender.send_dms_for_campaign(campaign_id, max_dms_per_account)
        logging.info(f"DM campaign {campaign_id} completed")
    except Exception as e:
        logging.error(f"DM campaign {campaign_id} failed: {e}")

@shared_task(rate_limit='50/h')
def enrich_user_details_task(account_id, source_type, source_value, batch_size=20):
    """Fetch detailed metadata for users in batches"""
    logger = logging.getLogger(__name__)
    try:
        account = Account.objects.get(id=account_id)
        if not account.can_scrape():
            logger.info(f"Account {account.username} cannot enrich users (health: {account.health_score}%)")
            return

        scraper = InstagramScraper()
        cl = setup_client(account)
        if not cl:
            logger.error(f"Failed to setup client for {account.username}")
            account.update_health_score(False, "scrape")
            return

        users = ScrapedUser.objects.filter(
            account=account,
            source_type=source_type,
            source_value=source_value,
            details_fetched=False,
            failure_reason__isnull=True
        )[:batch_size]

        updated_count = 0
        skipped_count = 0
        for user in users:
            try:
                user_data = scraper._get_user_detailed_info(cl, user.username)
                if not user_data:
                    user.failure_reason = "Failed to fetch detailed info"
                    user.is_active = False
                    user.details_fetched = True
                    user.save()
                    skipped_count += 1
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

                time.sleep(random.uniform(25, 30))
                if updated_count % 10 == 0:
                    time.sleep(random.uniform(120, 300))

            except Exception as e:
                logger.error(f"Failed to enrich user {user.username}: {e}")
                user.failure_reason = str(e)
                user.details_fetched = True
                user.save()
                skipped_count += 1
                continue

        logger.info(f"Enriched {updated_count} users, skipped {skipped_count} for {account.username}")
        account.update_health_score(True, "scrape")

        # Trigger classification if users were enriched
        if updated_count > 0:
            classify_users_task.delay(batch_size=batch_size)

        # Schedule another batch if more users remain
        if ScrapedUser.objects.filter(
                account=account,
                source_type=source_type,
                source_value=source_value,
                details_fetched=False
        ).exists():
            enrich_user_details_task.delay(account_id, source_type, source_value, batch_size)

    except Exception as e:
        logger.error(f"Enrichment task failed for account {account_id}: {e}")
        if account:
            account.status = "error"
            account.update_health_score(False, "scrape")
            account.save()
            send_alert(f"Enrichment task failed for {account.username}: {str(e)}", "error", account)

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