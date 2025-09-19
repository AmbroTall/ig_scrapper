## 6. Enhanced Tasks (tasks.py)

from celery import shared_task
from django.utils import timezone
from datetime import timedelta
import logging
import random
import time

from .models import ScrapedUser
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .filter import UserFilter
from .models import Account, DMCampaign, Alert
from .utils import setup_client


@shared_task
def scrape_users_task(account_id, source_type, source_id, amount=None):
    """Enhanced scraping task with better error handling"""

    try:
        account = Account.objects.get(id=account_id)
    except Account.DoesNotExist:
        logging.error(f"Account {account_id} not found")
        return

    scraper = InstagramScraper()

    try:
        # Random delay to spread task execution
        delay = random.randint(10, 60)
        time.sleep(delay)

        # Collect usernames
        usernames = scraper.collect_usernames(account, source_type, source_id, amount)

        # Store users with metadata
        if usernames:
            scraper.store_users_enhanced(usernames, account, source_type, source_id)

        # Trigger expansion immediately
        expand_users_task.delay(account.id)

        logging.info(f"Scraping task completed for {account.username}: {len(usernames)} users")

    except Exception as e:
        logging.error(f"Scraping task failed for {account.username}: {e}")
        account.status = "error"
        account.save()


@shared_task
def send_dms_task(campaign_id, max_dms_per_account=15):
    """Send DMs for a campaign"""

    dm_sender = DMSender()

    try:
        dm_sender.send_dms_for_campaign(campaign_id, max_dms_per_account)
        logging.info(f"DM campaign {campaign_id} completed")

    except Exception as e:
        logging.error(f"DM campaign {campaign_id} failed: {e}")


@shared_task
def classify_users_task(batch_size=100):
    """Classify users using AI"""

    user_filter = UserFilter()

    try:
        classified = user_filter.classify_users_ai(batch_size)
        logging.info(f"Classification task completed: {classified} users classified")

    except Exception as e:
        logging.error(f"Classification task failed: {e}")


@shared_task
def health_check_task():
    """Monitor account health and send alerts"""

    try:
        # Check for problematic accounts
        flagged_accounts = Account.objects.filter(
            health_score__lt=30
        )

        for account in flagged_accounts:
            Alert.objects.create(
                message=f"Account {account.username} has low health score: {account.health_score}%",
                severity='warning',
                account=account
            )

        # Check for banned accounts
        banned_accounts = Account.objects.filter(
            status='banned'
        )

        for account in banned_accounts:
            Alert.objects.create(
                message=f"Account {account.username} is banned",
                severity='critical',
                account=account
            )

        # Check login failures
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
def warmup_accounts_task():
    """Warm up accounts that need it"""

    scraper = InstagramScraper()

    try:
        # Find accounts that need warming up
        accounts_to_warmup = Account.objects.filter(
            warmed_up=False,
            status__in=['idle', 'error'],
            health_score__gt=50
        )[:5]  # Limit to 5 accounts per run

        for account in accounts_to_warmup:
            try:
                scraper.warm_up_account(account)
                time.sleep(random.randint(300, 600))  # 5-10 minutes between accounts

            except Exception as e:
                logging.error(f"Warmup failed for {account.username}: {e}")

        logging.info(f"Warmup task completed for {len(accounts_to_warmup)} accounts")

    except Exception as e:
        logging.error(f"Warmup task failed: {e}")


@shared_task
def daily_reset_task():
    """Reset daily counters and perform maintenance"""

    try:
        # Reset daily counters for all accounts
        Account.objects.all().update(
            users_scraped_today=0,
            dms_sent_today=0,
            last_reset=timezone.now().date()
        )

        # Reset hourly action counters
        Account.objects.all().update(
            actions_this_hour=0,
            hour_reset=timezone.now()
        )

        # Clean up old alerts
        old_alerts = Alert.objects.filter(
            timestamp__lt=timezone.now() - timedelta(days=30)
        )
        deleted_count = old_alerts.count()
        old_alerts.delete()

        logging.info(f"Daily reset completed. Cleaned up {deleted_count} old alerts")

    except Exception as e:
        logging.error(f"Daily reset failed: {e}")

@shared_task
def expand_users_task(account_id, batch_size=20):
    """Expand scraped users by exploring followers, followings, likers, commenters"""
    try:
        account = Account.objects.get(id=account_id)
    except Account.DoesNotExist:
        logging.error(f"Account {account_id} not found")
        return

    scraper = InstagramScraper()
    cl = setup_client(account)
    if not cl:
        return

    # Get users that need expansion
    users_to_expand = ScrapedUser.objects.filter(
        expanded=False
    ).order_by('scraped_at')[:batch_size]

    for user in users_to_expand:
        new_usernames = set()

        # Expand in multiple ways
        new_usernames |= scraper._scrape_followers(cl, user.username, 30)
        new_usernames |= scraper._scrape_following(cl, user.username, 30)

        # Optional: expand from recent posts
        try:
            medias = cl.user_medias_v1(user.user_id, 2)
            for m in medias:
                new_usernames |= scraper._scrape_likers(cl, m.pk, 30)
                new_usernames |= scraper._scrape_commenters(cl, m.pk, 30)
        except Exception as e:
            logging.warning(f"Could not expand media for {user.username}: {e}")

        # Store results
        if new_usernames:
            scraper.store_users_enhanced(new_usernames, account, "expansion", user.username)

        # Mark user as expanded
        user.expanded = True
        user.save()

        time.sleep(random.uniform(2, 5))  # avoid detection

    logging.info(f"Expanded {len(users_to_expand)} users for account {account.username}")

