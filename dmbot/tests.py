import time

from dmbot.scraper import InstagramScraper
from dmbot.models import Account, ScrapedUser
from dmbot.utils import setup_client

# Initialize scraper
scraper = InstagramScraper()

# Fetch one usable account
acc = Account.objects.get(id=5)
# acc = Account.objects.filter(status="idle", health_score__gt=50).last()

scraper.warm_up_account(acc)
time.sleep(50)

# Example: scrape 5 users from hashtag "fitness"
tags = scraper.collect_usernames(acc, "hashtag", "watermelon", amount=5)

# Store them
scraper.store_users_enhanced(tags, acc, "hashtag", "watermelon")

# Expand first 2 results (followers)
if tags:
    cl = setup_client(acc)
    for u in list(tags)[:2]:
        try:
            followers = scraper._scrape_followers(cl, u, 5)
            print(f"Expanded {u}: {len(followers)} followers")
            scraper.store_users_enhanced(followers, acc, "expansion", u)
        except Exception as e:
            print(f"Expansion failed for {u}: {e}")
