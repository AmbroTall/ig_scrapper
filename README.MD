# dmbot - Instagram Automation Bot

**dmbot** is a Django-based Instagram automation tool designed to scrape users from hashtag posts and send personalized direct messages (DMs) using the DeepSeek API. It supports managing 2000+ Instagram accounts, scraping ~700-1400 users per hashtag cycle (100-200 likers per 3-7 posts), and sending DMs with human-like behavior to avoid detection.

## Features

### Scraping
- Collects ~700-1400 usernames per hashtag cycle by scraping likers of top posts (100-200 likers per 3-7 posts)
- Stops scraping on ChallengeRequired errors
- Uses session reuse for efficiency
- Stores users in database for scalability

### DM Sending
- Sends personalized DMs using DeepSeek API
- Respects daily limits (e.g., 15 DMs/account)
- Template-based messaging system

### Account Management
- Supports 2000+ accounts with health scoring
- Rate limiting and session reuse
- Account status tracking and monitoring

### Human-Like Behavior
- Random delays (10-20s between liker chunks, 120-300s between posts)
- Warm-up activities (liking, browsing) to mimic human interaction
- Exponential backoff for rate limits (300-1200s)

### Error Handling & Scalability
- Comprehensive error logging and alerts
- Uses Celery for task distribution
- Django's ORM with bulk operations for efficient database management
- PostgreSQL/MySQL support for production

## Requirements

- Python 3.8+
- Django 3.2+
- Dependencies: `instagrapi`, `requests`, `celery`, `redis`
- Redis server for Celery task queue
- PostgreSQL/MySQL for database (SQLite for testing)
- DeepSeek API key for DM personalization
- Instagram accounts with valid credentials

## Setup

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd dmbot
```

### 2. Install Dependencies

Create a virtual environment and install requirements:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install instagrapi requests django celery redis
```

### 3. Configure Django Settings

Edit `dmbot/settings.py`:

```python
# dmbot/settings.py
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'dmbot',  # Your app
]

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',  # Use PostgreSQL/MySQL in production
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
CELERYD_CONCURRENCY = 8

SCRAPING_LIMIT_PER_SOURCE = 1000
DEEPSEEK_API_KEY = 'your-deepseek-api-key'  # Replace with actual key
DELAY_RANGE = [2, 5]

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'dmbot': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    },
}
```

### 4. Install and Start Redis

```bash
sudo apt install redis-server  # On Linux
# On macOS: brew install redis
# On Windows: Use WSL or a Redis Docker container
redis-server  # Start Redis
```

### 5. Apply Database Migrations

```bash
python manage.py makemigrations
python manage.py migrate
```

### 6. Create Sessions Directory

```bash
mkdir sessions
chmod 777 sessions  # Ensure writable
```

## Usage

### Running via Django Shell

Test the bot's scraping and DM-sending functionality using the Django shell.

#### 1. Start the Django Shell

```bash
python manage.py shell
```

#### 2. Create a Test Account

Add an Instagram account to the database:

```python
from dmbot.models import Account
from django.utils import timezone

account, created = Account.objects.update_or_create(
    username="test_instagram_account",  # Replace with real username
    defaults={
        "password": "your_instagram_password",  # Replace with real password
        "status": "idle",
        "health_score": 100,
        "daily_scrape_limit": 1000,
        "daily_dm_limit": 50,
        "warmed_up": False,  # Bot will warm up if needed
        "account_age_days": 90,
        "last_active": timezone.now(),
        "hour_reset": timezone.now(),
        "actions_this_hour": 0,
        "users_scraped_today": 0,
        "dms_sent_today": 0,
    }
)
print(f"Account {'created' if created else 'updated'}: {account.username}")
```

#### 3. Test Scraping

Scrape users for a hashtag (e.g., "developer"):

```python
from dmbot.scraper import InstagramScraper
from dmbot.models import ScrapedUser, Alert

scraper = InstagramScraper()
account = Account.objects.get(username="test_instagram_account")
source_type = "hashtag"
source_id = "developer"
amount = 1000

usernames = scraper.collect_usernames(account, source_type, source_id, amount)
if usernames:
    scraper.store_users_enhanced(usernames, account, source_type, source_id)
    print(f"Scraped and stored {len(usernames)} users for hashtag: {source_id}")
else:
    print("No users scraped")

user_count = ScrapedUser.objects.filter(account=account, source_type=source_type, source_value=source_id).count()
print(f"Total users in database: {user_count}")

alerts = Alert.objects.filter(account=account).order_by('-created_at')[:5]
for alert in alerts:
    print(f"Alert [{alert.severity}]: {alert.message} at {alert.created_at}")
```

**Expected:** ~700-1400 users scraped, stored in ScrapedUser. Stops on ChallengeRequired with status="flagged".

#### 4. Test DM Sending

Create a DM campaign and send messages:

```python
from dmbot.models import DMTemplate, DMCampaign
from dmbot.dm_sender import DMSender

# Create DM template
template, created = DMTemplate.objects.update_or_create(
    name="test_template",
    defaults={
        "template": "Hey [name], loved your bio about [bio_keyword]! Let's connect!",
        "success_rate": 0.0,
        "times_used": 0,
    }
)
print(f"Template {'created' if created else 'updated'}: {template.name}")

# Create DM campaign
campaign, created = DMCampaign.objects.update_or_create(
    name="test_campaign",
    defaults={
        "template": template,
        "is_active": True,
        "max_dms_per_account": 15,
        "total_sent": 0,
        "response_rate": 0.0,
    }
)
campaign.accounts.add(Account.objects.get(username="test_instagram_account"))
print(f"Campaign {'created' if created else 'updated'}: {campaign.name}")

# Send DMs
dm_sender = DMSender()
dm_sender.send_dms_for_campaign(campaign.id, max_dms_per_account=15)

sent_users = ScrapedUser.objects.filter(dm_sent=True, dm_account=account).count()
print(f"DMs sent: {sent_users}")

campaign.refresh_from_db()
print(f"Campaign total sent: {campaign.total_sent}")

alerts = Alert.objects.filter(account=account).order_by('-created_at')[:5]
for alert in alerts:
    print(f"Alert [{alert.severity}]: {alert.message} at {alert.created_at}")
```

**Expected:** Up to 15 DMs sent, personalized with DeepSeek. Stops on ChallengeRequired.

#### 5. Test Multiple Accounts

Test with 2-3 accounts (scalable to 2000+):

```python
from dmbot.models import Account
from dmbot.scraper import InstagramScraper

# Add more accounts
usernames = ["test_account2", "test_account3"]  # Replace with real usernames
for username in usernames:
    Account.objects.update_or_create(
        username=username,
        defaults={
            "password": "your_password",  # Replace with real password
            "status": "idle",
            "health_score": 100,
            "daily_scrape_limit": 1000,
            "daily_dm_limit": 50,
            "warmed_up": False,
            "account_age_days": 90,
            "last_active": timezone.now(),
            "hour_reset": timezone.now(),
            "actions_this_hour": 0,
            "users_scraped_today": 0,
            "dms_sent_today": 0,
        }
    )

# Scrape with multiple accounts
scraper = InstagramScraper()
source_type = "hashtag"
source_id = "developer"
amount = 1000

for account in Account.objects.filter(status="idle"):
    print(f"Scraping for {account.username}")
    usernames = scraper.collect_usernames(account, source_type, source_id, amount)
    if usernames:
        scraper.store_users_enhanced(usernames, account, source_type, source_id)
        print(f"Scraped {len(usernames)} users for {account.username}")
    account.refresh_from_db()
    print(f"Status: {account.status}, Health: {account.health_score}")
    alerts = Alert.objects.filter(account=account).order_by('-created_at')[:5]
    for alert in alerts:
        print(f"Alert [{alert.severity}]: {alert.message}")

total_users = ScrapedUser.objects.filter(source_type=source_type, source_value=source_id).count()
print(f"Total users scraped: {total_users}")
```

**Expected:** ~2100-4200 users across 3 accounts. Stops on ChallengeRequired.

### Running in Production

#### Start Celery:

```bash
celery -A dmbot worker --loglevel=info --concurrency=8
```

#### Run Django Server:

```bash
python manage.py runserver
```

#### Use Django Admin:

- Access `/admin` to manage Account, ScrapedUser, DMTemplate, DMCampaign, and Alert models
- Upload accounts via CSV (requires AccountUploadView)
- Start scraping/DM tasks via UI or Celery

#### Scaling to 2000+ Accounts:

- Use multiple Celery workers (e.g., 4 workers × 8 concurrency)
- Ensure database indexes on `Account(username, status)`, `ScrapedUser(username, dm_sent, account)`
- Monitor Alert objects for flagged or rate_limited accounts

## Important Notes

### Challenge Handling
The bot stops scraping on ChallengeRequired errors, marking accounts as flagged. Manually resolve challenges via the Instagram app and reset `status="idle"`, `health_score=100`.

### Rate Limits
Uses 50 API calls/hour/account and exponential backoff (300-1200s) to avoid IP bans.

### Low-Yield Hashtags
Skips posts with <20 likers, fetches up to 10 posts, or falls back to search if yield is low.

### Risks Without Proxies
Running 2000 accounts on one IP increases detection risk. Consider a small proxy pool if issues arise.

## Troubleshooting

### ChallengeRequired
Check Alert objects and reset flagged accounts:

```python
account = Account.objects.get(username="test_instagram_account")
account.status = "idle"
account.health_score = 100
account.save()
```

### Rate Limits
Wait 5-20 minutes or reset `actions_this_hour=0`, `hour_reset=timezone.now()`.

### Low Yield
Try popular hashtags (e.g., "fitness") or request related hashtag scraping logic.

### Session Issues
Ensure `sessions/` is writable. Delete stale `.json` files to force fresh login.

## Monitoring Commands

Check various aspects of your bot:

```python
# Check ScrapedUser count
ScrapedUser.objects.count()

# Check account status
Account.objects.values('username', 'status', 'health_score')

# Check recent alerts
Alert.objects.all().order_by('-created_at')[:10]
```

## Contributing

Submit issues or PRs to `<your-repo-url>`. For feature requests (e.g., higher liker limits, challenge handling), contact the maintainer.

## License

MIT License

---

**⚠️ Disclaimer:** This tool is for educational purposes. Use responsibly and in compliance with Instagram's Terms of Service. The authors are not responsible for any consequences arising from the use of this software.