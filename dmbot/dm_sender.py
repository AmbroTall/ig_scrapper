import logging
import random
import time
from django.core.cache import cache
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, RateLimitError
from .filter import UserFilter
from .models import DMTemplate, DMCampaign, DMLog, Alert, ScrapedUser
from .utils import setup_client, send_alert
import os
from openai import OpenAI

class DMSender:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.deepseek_api_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
        self.proxies = getattr(settings, 'PROXIES', [])
        # Initialize OpenAI client for DeepSeek
        self.client = OpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )

    def generate_dynamic_dm(self, bio, template):
        """Generate personalized DM using DeepSeek with OpenAI SDK"""
        if not bio:
            self.logger.info("Empty bio, using raw template")
            send_alert("Empty bio, using raw template", "info")
            return template
        cache_key = f"dm_{hash(bio)}_{hash(template)}"
        cached_dm = cache.get(cache_key)
        if cached_dm:
            self.logger.info("Using cached DM")
            send_alert("Using cached DM", "info")
            return cached_dm
        try:
            prompt = f"Generate a friendly, concise Instagram DM (under 100 chars) based on template: '{template}'. Personalize for user bio: '{bio}'. Avoid salesy tone, sound human."
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=100,
                temperature=0.7,
                stream=False
            )
            dm_text = response.choices[0].message.content.strip()
            cache.set(cache_key, dm_text, timeout=3600)
            return dm_text
        except Exception as e:
            self.logger.error(f"DeepSeek DM generation failed: {e}")
            send_alert(f"DeepSeek DM generation failed: {e}", "error")
            return template

    def perform_activity(self, cl, account, campaign):
        """Perform light Instagram activity to mimic human behavior"""
        try:
            self.logger.info(f"Performing activity for {account.username}")
            send_alert(f"Performing activity for {account.username}", "info", account)

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
            send_alert(f"Activity failed for {account.username}: {e}", "error", account)

    def _browse_hashtags(self, cl, count=3):
        try:
            hashtags = ["art", "design", "photography"]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                self.logger.info(f"Browsed hashtag: {hashtag}")
                send_alert(f"Browsed hashtag: {hashtag}", "info")
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")
            send_alert(f"Browse hashtags failed: {e}", "info")

    def _view_stories(self, cl, count=3):
        try:
            stories = cl.timeline_feed()[:count]
            if stories:
                for story in stories:
                    # Optionally: cl.story_view(story.pk)
                    self.logger.info(f"Viewed story {story.get('pk', 'unknown')}")
                    send_alert(f"Viewed story {story.get('pk', 'unknown')}", "info")
                    time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")
            send_alert(f"View stories failed: {e}", "info")

    def send_dms_for_campaign(self, campaign_id, max_dms_per_account=15):
        try:
            campaign = DMCampaign.objects.get(id=campaign_id)
            if not campaign.is_active:
                self.logger.info(f"Campaign {campaign.name} is inactive")
                send_alert(f"Campaign {campaign.name} is inactive", "info")
                return

            # Check for active template
            if not DMTemplate.objects.filter(id=campaign.template_id, active=True).exists():
                self.logger.error(f"No active template for campaign {campaign.name}")
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
                is_private=False  # Exclude private users
            )
            self.logger.info(f"Found {users.count()} eligible users for campaign {campaign.name}")
            send_alert(f"Found {users.count()} eligible users for campaign {campaign.name}", "info")

            # Fallback to users with details if no filtered users
            if not users.exists():
                self.logger.warning(
                    f"No users match campaign filters for {campaign.name}, falling back to users with details")
                send_alert(
                    f"No users match campaign filters for {campaign.name}, falling back to users with details",
                    "warning"
                )
                users = ScrapedUser.objects.filter(
                    dm_sent=False,
                    is_active=True,
                    details_fetched=True,
                    is_private=False  # Exclude private users
                )
                if not users.exists():
                    self.logger.error(f"No eligible users available for campaign {campaign.name}")
                    send_alert(f"No eligible users available for campaign {campaign.name}", "error")
                    campaign.is_active = False
                    campaign.save()
                    return

            for account in accounts:
                if not account.can_send_dm():
                    self.logger.info(f"Account {account.username} cannot send DMs")
                    send_alert(f"Account {account.username} cannot send DMs", "info", account)
                    continue
                cl = setup_client(account)
                if not cl:
                    account.update_health_score(False, "dm")
                    continue

                # Distribute users across accounts
                user_batch = users[:max_dms_per_account]
                self.logger.info(f"Assigned {user_batch.count()} users to account {account.username}")
                send_alert(f"Assigned {user_batch.count()} users to account {account.username}", "info", account)
                if not user_batch.exists():
                    self.logger.warning(f"No users available for account {account.username}, skipping")
                    send_alert(f"No users available for account {account.username}, skipping", "warning", account)
                    continue

                account.status = "sending_dms"
                account.save()
                dm_count = 0
                for user in user_batch:
                    alert = Alert.objects.create(
                        account=account,
                        severity='info',
                        message=f"Sending DM to {user.username} for campaign {campaign.name}",
                        timestamp=timezone.now()
                    )
                    try:
                        user_id = cl.user_id_from_username(user.username)
                        dm_text = self.generate_dynamic_dm(user.biography, template)
                        cl.direct_send(dm_text, [user_id])
                        user.dm_sent = True
                        user.dm_sent_at = timezone.now()
                        user.dm_account = account
                        user.save()
                        DMLog.objects.create(
                            sender_account=account,
                            recipient_user=user,
                            message=dm_text,
                            sent_at=timezone.now()
                        )
                        campaign.total_sent += 1
                        account.dms_sent_today += 1
                        account.last_active = timezone.now()
                        account.update_health_score(True, "dm")
                        alert.message += "\nDM sent successfully"
                        alert.save()
                        self.logger.info(f"Sent DM to {user.username} from {account.username}")
                        send_alert(f"Sent DM to {user.username} from {account.username}", "info", account)
                        dm_count += 1
                        if dm_count % 3 == 0:
                            self.perform_activity(cl, account, campaign)
                        time.sleep(random.uniform(40, 120))
                    except RateLimitError:
                        account.status = "rate_limited"
                        account.save()
                        send_alert(f"Rate limit hit for DMs on {account.username}", "warning", account)
                        alert.message += "\nFailed: Rate limit hit"
                        alert.severity = 'warning'
                        alert.save()
                        break
                    except ClientError as e:
                        error_str = str(e).lower()
                        if "403" in error_str:
                            self.logger.info(f"Cannot send DM to {user.username} due to 403 error, skipping")
                            send_alert(f"Cannot send DM to {user.username} due to 403 error, skipping", "info", account)
                            user.is_active = False
                            user.failure_reason = "403 Forbidden: DM restricted"
                            user.save()
                            alert.message += "\nFailed: 403 Forbidden, user skipped"
                            alert.severity = 'info'
                            alert.save()
                        else:
                            self.logger.error(f"Failed to send DM to {user.username}: {e}")
                            send_alert(f"Failed to send DM to {user.username}: {e}", "error", account)
                            account.update_health_score(False, "dm")
                            alert.message += f"\nFailed: {str(e)}"
                            alert.severity = 'error'
                            alert.save()
                    except Exception as e:
                        self.logger.error(f"Unexpected DM error for {user.username}: {e}")
                        send_alert(f"Unexpected DM error for {user.username}: {e}", "error", account)
                        user.failure_reason = str(e)
                        user.save()
                        alert.message += f"\nFailed: {str(e)}"
                        alert.severity = 'error'
                        alert.save()
                    users = users.exclude(id=user.id)  # Remove sent or skipped user
                account.status = "idle"
                account.save()

            # Check if campaign is complete (no eligible users left)
            remaining_users = user_filter.filter_users(
                professions=campaign.target_filters.get('professions', []),
                countries=campaign.target_filters.get('countries', []),
                keywords=campaign.target_filters.get('keywords', []),
                activity_days=30
            ).filter(
                dm_sent=False,
                is_active=True,
                is_private=False  # Exclude private users
            )
            if not remaining_users.exists():
                self.logger.info(f"Campaign {campaign.name} completed: no eligible users remaining")
                send_alert(f"Campaign {campaign.name} completed: no eligible users remaining", "info")
                campaign.is_active = False
            campaign.save()

        except Exception as e:
            self.logger.error(f"DM campaign {campaign_id} failed: {e}")
            send_alert(f"DM campaign {campaign_id} failed: {str(e)}", "error")
            try:
                campaign = DMCampaign.objects.get(id=campaign_id)
                campaign.is_active = False
                campaign.save()
            except DMCampaign.DoesNotExist:
                self.logger.error(f"Campaign {campaign_id} does not exist")
                send_alert(f"Campaign {campaign_id} does not exist", "error")