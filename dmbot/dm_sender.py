import logging
import random
import time
from django.core.cache import cache
from django.utils import timezone
from django.conf import settings
from django.db import transaction
from instagrapi.exceptions import ClientError, RateLimitError
from .filter import UserFilter
from .models import DMTemplate, DMCampaign, DMLog, Alert, ScrapedUser, Account, FallbackMessage
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
            cache_key = f"alert_empty_bio_{hash(template)}"
            if not cache.get(cache_key):
                send_alert("Empty bio, using raw template", "info")
                cache.set(cache_key, True, timeout=60)
            return template
        cache_key = f"dm_{hash(bio)}_{hash(template)}"
        cached_dm = cache.get(cache_key)
        if cached_dm:
            self.logger.info("Using cached DM")
            cache_key = f"alert_cached_dm_{hash(bio)}_{hash(template)}"
            if not cache.get(cache_key):
                send_alert("Using cached DM", "info")
                cache.set(cache_key, True, timeout=60)
            return cached_dm
        try:
            prompt = f"Generate a friendly, concise Instagram DM (brief warm message) based on template: '{template}'. Personalize for user bio: '{bio}'. Avoid salesy tone, sound human."
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
            cache_key = f"alert_dm_generation_failed_{hash(bio)}_{hash(template)} sending default message"
            if not cache.get(cache_key):
                send_alert(f"DeepSeek DM generation failed: {e}", "error")
                cache.set(cache_key, True, timeout=60)
            return self.get_fallback_message()

    def get_fallback_message(self):
        """Get active fallback or default"""
        fallback = FallbackMessage.objects.filter(is_active=True).first()
        if fallback:
            return fallback.message
        return "Hey! Loved your profile — let's connect"

    def perform_activity(self, cl, account, campaign):
        """Perform light Instagram activity to mimic human behavior"""
        try:
            self.logger.info(f"Performing activity for {account.username}")
            cache_key = f"alert_perform_activity_{account.id}"
            if not cache.get(cache_key):
                send_alert(f"Performing activity for {account.username}", "info", account)
                cache.set(cache_key, True, timeout=60)

            # Randomize activity counts between 2–6
            hashtag_count = random.randint(2, 6)
            story_count = random.randint(2, 6)

            # Browse hashtags
            self._browse_hashtags(cl, count=hashtag_count)
            with transaction.atomic():
                account = Account.objects.select_for_update().get(id=account.id)
                account.last_active = timezone.now()
                account.update_health_score(True, "browse_hashtags")
                account.save()

            # View stories
            self._view_stories(cl, count=story_count)
            with transaction.atomic():
                account = Account.objects.select_for_update().get(id=account.id)
                account.last_active = timezone.now()
                account.update_health_score(True, "view_stories")
                account.save()

        except Exception as e:
            self.logger.error(f"Activity failed for {account.username}: {e}", exc_info=True)
            cache_key = f"alert_activity_failed_{account.id}"
            if not cache.get(cache_key):
                send_alert(f"Activity failed for {account.username}: {e}", "error", account)
                cache.set(cache_key, True, timeout=60)

    def _browse_hashtags(self, cl, count=3):
        try:
            hashtags = [
                "art", "design", "photography", "artist", "artwork", "creative", "illustration",
                "drawing", "painting", "digitalart", "graphicdesign", "artoftheday", "photooftheday",
                "fineart", "modernart", "contemporaryart", "artgallery", "artcollectors",
                "artlovers", "instaart", "instadesign", "instaphotography", "visualart",
                "aesthetic", "creativity", "inspiration", "sketch", "sketchbook", "portrait",
                "landscape", "abstract", "minimalism", "designinspiration", "interiordesign",
                "architecture", "branding", "logo", "typography", "motiondesign", "animation",
                "3dart", "3dmodeling", "digitalpainting", "conceptart", "characterdesign",
                "photographer", "photoart", "streetphotography", "travelphotography",
                "naturephotography", "blackandwhite", "bnw", "portraitphotography",
                "fashionphotography", "filmphotography", "macro", "nightphotography",
                "artcommunity", "artlife", "creativeprocess", "artistic", "mixedmedia",
                "surrealism", "expressionism", "popart", "realism", "gallery", "museum",
                "artshow", "artstudio", "artoftheworld", "designlife", "uxdesign", "uidesign",
                "productdesign", "industrialdesign", "graphicdesigner", "photoshoot", "lens",
                "dslr", "mirrorless", "lightroom", "photoshop", "capture", "composition",
                "color", "visualdesign", "artdirection", "creativedirection", "illustrator",
                "artistsoninstagram", "designers", "creatives", "handmade", "craft", "collage",
                "digitalillustration", "drawingoftheday", "artdaily", "photodaily",
                "fineartphotography", "artgram", "creativeart", "instaartist", "artoftheweek",
                "designcommunity", "colorgrading", "photoediting", "creativephotography",
                "editorialdesign", "fashiondesign", "artjournal", "paintingart", "artcollection",
                "inspired", "designstudio", "creativeindustry", "illustrationart", "designthinking",
                "artworld", "photojournalism", "visualstorytelling", "photoediting", "canon",
                "nikon", "sonyalpha", "fujifilm", "leica", "cinematography", "streetart",
                "graffiti", "urbanart", "digitalcreator", "contentcreator", "visuals", "aestheticart"
            ]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                self.logger.info(f"Browsed hashtag: {hashtag}")
                cache_key = f"alert_browse_hashtag_{hashtag}"
                if not cache.get(cache_key):
                    send_alert(f"Browsed hashtag: {hashtag}", "info")
                    cache.set(cache_key, True, timeout=60)
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")
            cache_key = f"alert_browse_hashtags_failed_{hash(str(e))}"
            if not cache.get(cache_key):
                send_alert(f"Browse hashtags failed: {e}", "info")
                cache.set(cache_key, True, timeout=60)

    def _view_stories(self, cl, count=3):
        try:
            stories = cl.timeline_feed()[:count]
            if stories:
                for story in stories:
                    # Optionally: cl.story_view(story.pk)
                    self.logger.info(f"Viewed story {story.get('pk', 'unknown')}")
                    cache_key = f"alert_view_story_{story.get('pk', 'unknown')}"
                    if not cache.get(cache_key):
                        send_alert(f"Viewed story {story.get('pk', 'unknown')}", "info")
                        cache.set(cache_key, True, timeout=60)
                    time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")
            cache_key = f"alert_view_stories_failed_{hash(str(e))}"
            if not cache.get(cache_key):
                send_alert(f"View stories failed: {e}", "info")
                cache.set(cache_key, True, timeout=60)

    def send_dms_for_campaign(self, campaign_id, max_dms_per_account=15):
        try:
            with transaction.atomic():
                campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                if not campaign.is_active:
                    self.logger.info(f"Campaign {campaign.name} is inactive")
                    cache_key = f"alert_campaign_inactive_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"Campaign {campaign.name} is inactive", "info")
                        cache.set(cache_key, True, timeout=60)
                    return

                # Check for active template
                if not DMTemplate.objects.filter(id=campaign.template_id, active=True).exists():
                    self.logger.error(f"No active template for campaign {campaign.name}")
                    cache_key = f"alert_no_active_template_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"No active template for campaign {campaign.name}", "error")
                        cache.set(cache_key, True, timeout=60)
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
            cache_key = f"alert_eligible_users_{campaign_id}"
            if not cache.get(cache_key):
                send_alert(f"Found {users.count()} eligible users for campaign {campaign.name}", "info")
                cache.set(cache_key, True, timeout=60)

            # Fallback to users with details if no filtered users
            if not users.exists():
                self.logger.warning(
                    f"No users match campaign filters for {campaign.name}, falling back to users with details")
                cache_key = f"alert_fallback_users_{campaign_id}"
                if not cache.get(cache_key):
                    send_alert(
                        f"No users match campaign filters for {campaign.name}, falling back to users with details",
                        "warning"
                    )
                    cache.set(cache_key, True, timeout=60)
                users = ScrapedUser.objects.filter(
                    dm_sent=False,
                    is_active=True,
                    details_fetched=True,
                    is_private=False  # Exclude private users
                )
                if not users.exists():
                    self.logger.error(f"No eligible users available for campaign {campaign.name}")
                    cache_key = f"alert_no_users_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"No eligible users available for campaign {campaign.name}", "error")
                        cache.set(cache_key, True, timeout=60)
                    with transaction.atomic():
                        campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                        campaign.is_active = False
                        campaign.save()
                    return

            for account in accounts:
                if not account.can_send_dm():
                    self.logger.info(f"Account {account.username} cannot send DMs")
                    cache_key = f"alert_account_cannot_dm_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Account {account.username} cannot send DMs", "info", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                cl = setup_client(account)
                if not cl:
                    with transaction.atomic():
                        account = Account.objects.select_for_update().get(id=account.id)
                        account.update_health_score(False, "dm")
                        account.save()
                    cache_key = f"alert_client_setup_fail_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Failed to setup client for {account.username}", "error", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                # Distribute users across accounts
                user_batch = users[:max_dms_per_account]
                self.logger.info(f"Assigned {user_batch.count()} users to account {account.username}")
                cache_key = f"alert_assigned_users_{account.id}_{campaign_id}"
                if not cache.get(cache_key):
                    send_alert(f"Assigned {user_batch.count()} users to account {account.username}", "info", account)
                    cache.set(cache_key, True, timeout=60)
                if not user_batch.exists():
                    self.logger.warning(f"No users available for account {account.username}, skipping")
                    cache_key = f"alert_no_users_account_{account.id}_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"No users available for account {account.username}, skipping", "warning", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                with transaction.atomic():
                    account = Account.objects.select_for_update().get(id=account.id)
                    account.status = "sending_dms"
                    account.save()

                dm_count = 0
                for user in user_batch:
                    try:
                        with transaction.atomic():
                            alert = Alert.objects.create(
                                account=account,
                                severity="info",
                                message=f"Sending DM to {user.username} for campaign {campaign.name}",
                                timestamp=timezone.now()
                            )
                            user = ScrapedUser.objects.select_for_update().get(id=user.id)
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
                            campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                            campaign.total_sent += 1
                            campaign.save()
                            account = Account.objects.select_for_update().get(id=account.id)
                            account.dms_sent_today += 1
                            account.last_active = timezone.now()
                            account.update_health_score(True, "dm")
                            account.save()
                            alert.message += "\nDM sent successfully"
                            alert.save()

                        self.logger.info(f"Sent DM to {user.username} from {account.username}")
                        cache_key = f"alert_dm_sent_{user.username}_{account.id}_{campaign_id}"
                        if not cache.get(cache_key):
                            send_alert(f"Sent DM to {user.username} from {account.username}", "info", account)
                            cache.set(cache_key, True, timeout=60)
                        dm_count += 1
                        if dm_count % 3 == 0:
                            self.perform_activity(cl, account, campaign)
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
                        cache_key = f"alert_rate_limit_dm_{account.id}_{campaign_id}"
                        if not cache.get(cache_key):
                            send_alert(f"Rate limit hit for DMs on {account.username}", "warning", account)
                            cache.set(cache_key, True, timeout=60)
                        break
                    except ClientError as e:
                        error_str = str(e).lower()
                        with transaction.atomic():
                            user = ScrapedUser.objects.select_for_update().get(id=user.id)
                            alert = Alert.objects.select_for_update().get(id=alert.id)
                            if "403" in error_str:
                                self.logger.info(f"Cannot send DM to {user.username} due to 403 error, skipping")
                                cache_key = f"alert_403_error_{user.username}_{account.id}_{campaign_id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Cannot send DM to {user.username} due to 403 error, skipping", "info",
                                               account)
                                    cache.set(cache_key, True, timeout=60)
                                user.is_active = False
                                user.failure_reason = "403 Forbidden: DM restricted"
                                user.save()
                                alert.message += "\nFailed: 403 Forbidden, user skipped"
                                alert.severity = "info"
                                alert.save()
                            else:
                                self.logger.error(f"Failed to send DM to {user.username}: {e}")
                                cache_key = f"alert_dm_error_{user.username}_{account.id}_{campaign_id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Failed to send DM to {user.username}: {e}", "error", account)
                                    cache.set(cache_key, True, timeout=60)
                                account = Account.objects.select_for_update().get(id=account.id)
                                account.update_health_score(False, "dm")
                                account.save()
                                alert.message += f"\nFailed: {str(e)}"
                                alert.severity = "error"
                                alert.save()
                    except Exception as e:
                        self.logger.error(f"Unexpected DM error for {user.username}: {e}")
                        cache_key = f"alert_unexpected_dm_error_{user.username}_{account.id}_{campaign_id}"
                        if not cache.get(cache_key):
                            send_alert(f"Unexpected DM error for {user.username}: {e}", "error", account)
                            cache.set(cache_key, True, timeout=60)
                        with transaction.atomic():
                            user = ScrapedUser.objects.select_for_update().get(id=user.id)
                            user.failure_reason = str(e)
                            user.save()
                            alert = Alert.objects.select_for_update().get(id=alert.id)
                            alert.message += f"\nFailed: {str(e)}"
                            alert.severity = "error"
                            alert.save()

                with transaction.atomic():
                    account = Account.objects.select_for_update().get(id=account.id)
                    account.status = "idle"
                    account.save()

            # Check if campaign is complete (no eligible users left)
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
                    self.logger.info(f"Campaign {campaign.name} completed: no eligible users remaining")
                    cache_key = f"alert_campaign_complete_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"Campaign {campaign.name} completed: no eligible users remaining", "info")
                        cache.set(cache_key, True, timeout=60)
                    campaign.is_active = False
                campaign.save()

        except Exception as e:
            self.logger.error(f"DM campaign {campaign_id} failed: {e}")
            cache_key = f"alert_campaign_failed_{campaign_id}"
            if not cache.get(cache_key):
                send_alert(f"DM campaign {campaign_id} failed: {str(e)}", "error")
                cache.set(cache_key, True, timeout=60)
            with transaction.atomic():
                try:
                    campaign = DMCampaign.objects.select_for_update().get(id=campaign_id)
                    campaign.is_active = False
                    campaign.save()
                except DMCampaign.DoesNotExist:
                    self.logger.error(f"Campaign {campaign_id} does not exist")
                    cache_key = f"alert_campaign_not_found_{campaign_id}"
                    if not cache.get(cache_key):
                        send_alert(f"Campaign {campaign_id} does not exist", "error")
                        cache.set(cache_key, True, timeout=60)
