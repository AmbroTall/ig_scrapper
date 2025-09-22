import logging
import random
import time
import requests
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, RateLimitError
from .models import ScrapedUser, DMTemplate, DMCampaign, Account
from .utils import setup_client, send_alert

class DMSender:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.deepseek_api_key = getattr(settings, 'DEEPSEEK_API_KEY', None)
        self.proxies = getattr(settings, 'PROXIES', [])

    def generate_dynamic_dm(self, bio, template):
        """Generate personalized DM using DeepSeek"""
        try:
            prompt = f"Create a concise, friendly Instagram DM based on this template: '{template}'. Personalize it using the user's bio: '{bio}'. Keep it natural and under 100 characters."
            response = requests.post(
                'https://api.deepseek.com/v1/completions',
                headers={'Authorization': f'Bearer {self.deepseek_api_key}'},
                json={'prompt': prompt, 'max_tokens': 100, 'temperature': 0.7},
                proxies=random.choice(self.proxies) if self.proxies else None
            )
            response.raise_for_status()
            return response.json()['choices'][0]['text'].strip()
        except Exception as e:
            self.logger.error(f"DeepSeek DM generation failed: {e}")
            return template  # Fallback to template

    def send_dms_for_campaign(self, campaign_id, max_dms_per_account=15):
        """Send DMs for a campaign"""
        try:
            campaign = DMCampaign.objects.get(id=campaign_id)
            if not campaign.is_active:
                self.logger.info(f"Campaign {campaign.name} is inactive")
                return
            template = campaign.template.template
            accounts = campaign.accounts.filter(status="idle", warmed_up=True)
            users = ScrapedUser.objects.filter(
                dm_sent=False, is_active=True, account__in=accounts
            ).exclude(failure_reason__isnull=False)[:max_dms_per_account * accounts.count()]
            for account in accounts:
                if not account.can_send_dm():
                    self.logger.info(f"Account {account.username} cannot send DMs")
                    continue
                cl = setup_client(account, proxy=random.choice(self.proxies) if self.proxies else None)
                if not cl:
                    account.update_health_score(False, "dm")
                    continue
                user_batch = users.filter(account=account)[:max_dms_per_account]
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
                        time.sleep(random.uniform(30, 60))  # Longer delay for DMs
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
                time.sleep(random.uniform(60, 120))  # Break between accounts
        except Exception as e:
            self.logger.error(f"DM campaign {campaign_id} failed: {e}")
            send_alert(f"DM campaign {campaign_id} failed: {str(e)}", "error")