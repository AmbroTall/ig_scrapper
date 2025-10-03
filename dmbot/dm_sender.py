import logging
import random
import time
import requests
from django.core import cache
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, RateLimitError
from .filter import UserFilter
from .models import DMTemplate, DMCampaign
from .utils import setup_client, send_alert


class DMSender:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.deepseek_api_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
        self.proxies = getattr(settings, 'PROXIES', [])

    def generate_dynamic_dm(self, bio, template):
        """Generate personalized DM using DeepSeek"""
        if not bio:
            self.logger.info("Empty bio, using raw template")
            return template
        cache_key = f"dm_{hash(bio)}_{hash(template)}"
        cached_dm = cache.get(cache_key)
        if cached_dm:
            self.logger.info("Using cached DM")
            return cached_dm
        try:
            prompt = f"Generate a friendly, concise Instagram DM (under 100 chars) based on template: '{template}'. Personalize for user bio: '{bio}'. Avoid salesy tone, sound human."
            response = requests.post(
                'https://api.deepseek.com/v1/completions',
                headers={'Authorization': f'Bearer {self.deepseek_api_key}'},
                json={'prompt': prompt, 'max_tokens': 100, 'temperature': 0.7},
                proxies=random.choice(self.proxies) if self.proxies else None
            )
            response.raise_for_status()
            dm_text = response.json()['choices'][0]['text'].strip()
            cache.set(cache_key, dm_text, timeout=3600)
            return dm_text
        except Exception as e:
            self.logger.error(f"DeepSeek DM generation failed: {e}")
            return template

    def perform_activity(self, cl, account, campaign):
        """Perform light Instagram activity to mimic human behavior"""
        try:
            self.logger.info(f"Performing activity for {account.username}")

            # Randomize activity counts between 2–6
            hashtag_count = random.randint(2, 6)
            story_count = random.randint(2, 6)

            # Browse hashtags
            self._browse_hashtags(cl, count=hashtag_count)
            account.last_active = timezone.now()
            account.update_health_score(True, "browse_hashtags")

            # View stories
            self._view_stories(cl, count=story_count)
            account.last_active = timezone.now()
            account.update_health_score(True, "view_stories")

        except Exception as e:
            self.logger.error(f"Activity failed for {account.username}: {e}", exc_info=True)

    def _browse_hashtags(self, cl, count=3):
        try:
            hashtags = ["art", "design", "photography"]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                self.logger.info(f"Browsed hashtag: {hashtag}")
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")

    def _view_stories(self, cl, count=3):
        try:
            stories = cl.timeline_feed()[:count]
            if stories:
                for story in stories:
                    # Optionally: cl.story_view(story.pk)
                    self.logger.info(f"Viewed story {story.get('pk', 'unknown')}")
                    time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")

    def send_dms_for_campaign(self, campaign_id, max_dms_per_account=15):
        """Send DMs for a campaign with interleaved activities"""
        try:
            campaign = DMCampaign.objects.get(id=campaign_id)
            if not campaign.is_active:
                self.logger.info(f"Campaign {campaign.name} is inactive")
                return
            if not DMTemplate.objects.filter(id=campaign.template_id, active=True).exists():
                self.logger.error(f"No active template for campaign {campaign.name}")
                send_alert(f"No active template for campaign {campaign.name}", "error")
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
                dm_sent=False, is_active=True, account__in=campaign.accounts.filter(status="idle", warmed_up=True)
            )[:max_dms_per_account * campaign.accounts.count()]
            for account in accounts:
                if not account.can_send_dm():
                    self.logger.info(f"Account {account.username} cannot send DMs")
                    continue
                cl = setup_client(account)
                if not cl:
                    account.update_health_score(False, "dm")
                    continue
                user_batch = users.filter(account=account)[:max_dms_per_account]
                account.status = "sending_dms"
                account.save()
                dm_count = 0
                for user in user_batch:
                    try:
                        dm_text = self.generate_dynamic_dm(user.biography, template)
                        cl.direct_send(dm_text, [cl.user_id_from_username(user.username)])
                        user.dm_sent = True
                        user.dm_sent_at = timezone.now()
                        user.dm_account = account
                        user.save()
                        campaign.total_sent += 1
                        account.dms_sent_today += 1
                        account.last_active = timezone.now()
                        account.update_health_score(True, "dm")
                        self.logger.info(f"Sent DM to {user.username} from {account.username}")
                        dm_count += 1
                        # Perform activity after every 3 DMs
                        if dm_count % 3 == 0:
                            self.perform_activity(cl, account, campaign)
                        time.sleep(random.uniform(40, 120))
                    except RateLimitError:
                        account.status = "rate_limited"
                        account.save()
                        send_alert(f"Rate limit hit for DMs on {account.username}", "warning", account)
                        time.sleep(random.uniform(300, 600))
                        break
                    except ClientError as e:
                        self.logger.error(f"Failed to send DM to {user.username}: {e}")
                        account.update_health_score(False, "dm")
                        send_alert(f"DM error for {account.username}: {str(e)}", "error", account)
                    except Exception as e:
                        self.logger.error(f"Unexpected DM error for {user.username}: {e}")
                        user.failure_reason = str(e)
                        user.save()
                campaign.save()
                account.save()
                time.sleep(random.uniform(60, 120))
        except Exception as e:
            self.logger.error(f"DM campaign {campaign_id} failed: {e}")
            send_alert(f"DM campaign {campaign_id} failed: {str(e)}", "error")