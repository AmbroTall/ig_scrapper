from celery import shared_task
from django.utils import timezone
import logging
import random
import time
from .models import ScrapedUser, Account, DMCampaign, Alert
from .scraper import InstagramScraper
from .dm_sender import DMSender
from .utils import setup_client


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
        expand_users_task.delay(account.id)
        logging.info(f"Scraping task completed for {account.username}: {len(usernames)} users")
    except Exception as e:
        logging.error(f"Scraping task failed for account {account_id}: {e}")
        if account:
            account.status = "error"
            account.save()

@shared_task
def send_dms_task(campaign_id, max_dms_per_account=15):
    dm_sender = DMSender()
    try:
        dm_sender.send_dms_for_campaign(campaign_id, max_dms_per_account)
        logging.info(f"DM campaign {campaign_id} completed")
    except Exception as e:
        logging.error(f"DM campaign {campaign_id} failed: {e}")

@shared_task
def expand_users_task(account_id, batch_size=20):
    try:
        account = Account.objects.get(id=account_id)
        scraper = InstagramScraper()
        cl = setup_client(account)
        if not cl:
            return
        users_to_expand = ScrapedUser.objects.filter(expanded=False, account=account)[:batch_size]
        for user in users_to_expand:
            new_usernames = set()
            new_usernames |= scraper._scrape_followers(cl, user.username, 30)
            new_usernames |= scraper._scrape_following(cl, user.username, 30)
            try:
                medias = cl.user_medias_v1(user.user_id, 2)
                for m in medias:
                    new_usernames |= scraper._scrape_likers(cl, m.pk, 30)
                    new_usernames |= scraper._scrape_commenters(cl, m.pk, 30)
            except Exception as e:
                logging.warning(f"Could not expand media for {user.username}: {e}")
            if new_usernames:
                scraper.store_users_enhanced(new_usernames, account, "expansion", user.username)
            user.expanded = True
            user.save()
            time.sleep(random.uniform(5, 10))
        logging.info(f"Expanded {len(users_to_expand)} users for account {account.username}")
    except Exception as e:
        logging.error(f"Expand users task failed for account {account_id}: {e}")