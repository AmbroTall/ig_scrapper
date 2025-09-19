## 4. DM Automation System (dm_sender.py)

import logging
import random
import time
from datetime import datetime, timedelta

from django.db import models
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, LoginRequired
from .models import ScrapedUser, DMTemplate, DMCampaign, Account
from .utils import setup_client, send_alert
import openai
import re


class DMSender:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def send_dms_for_campaign(self, campaign_id, max_dms_per_account=None):
        """Send DMs for a specific campaign"""
        try:
            campaign = DMCampaign.objects.get(id=campaign_id, is_active=True)
        except DMCampaign.DoesNotExist:
            self.logger.error(f"Campaign {campaign_id} not found or inactive")
            return

        # Get eligible users for this campaign
        eligible_users = self._get_eligible_users(campaign)
        if not eligible_users:
            self.logger.info(f"No eligible users found for campaign {campaign.name}")
            return

        # Get available accounts for sending
        available_accounts = [acc for acc in campaign.accounts.all() if acc.can_send_dm()]
        if not available_accounts:
            self.logger.info(f"No available accounts for campaign {campaign.name}")
            return

        # Distribute users across accounts
        users_per_account = max_dms_per_account or 15
        self._distribute_and_send_dms(eligible_users, available_accounts, campaign, users_per_account)

    def _get_eligible_users(self, campaign):
        """Get users eligible for DM based on campaign filters"""
        users = ScrapedUser.objects.filter(
            dm_sent=False,
            is_active=True
        )

        # Apply campaign filters
        filters = campaign.target_filters

        if filters.get('professions'):
            users = users.filter(profession__in=filters['professions'])

        if filters.get('countries'):
            users = users.filter(country__in=filters['countries'])

        if filters.get('keywords'):
            # Filter by keywords in biography
            keyword_query = None
            for keyword in filters['keywords']:
                q = models.Q(biography__icontains=keyword)
                keyword_query = keyword_query | q if keyword_query else q
            if keyword_query:
                users = users.filter(keyword_query)

        if filters.get('min_followers'):
            users = users.filter(follower_count__gte=filters['min_followers'])

        if filters.get('max_followers'):
            users = users.filter(follower_count__lte=filters['max_followers'])

        # Exclude users that received DMs recently (avoid spam)
        recent_dm_cutoff = timezone.now() - timedelta(days=7)
        users = users.exclude(
            dm_sent_at__gte=recent_dm_cutoff
        )

        return users.order_by('?')  # Random order

    def _distribute_and_send_dms(self, users, accounts, campaign, dms_per_account):
        """Distribute users across accounts and send DMs"""
        total_sent = 0

        # Create batches for each account
        user_batches = []
        for i, account in enumerate(accounts):
            start_idx = i * dms_per_account
            end_idx = start_idx + dms_per_account
            batch = list(users[start_idx:end_idx])
            if batch:
                user_batches.append((account, batch))

        # Send DMs in parallel-like fashion with delays
        for account, user_batch in user_batches:
            sent_count = self._send_dm_batch(account, user_batch, campaign)
            total_sent += sent_count

            # Random delay between account switches
            time.sleep(random.randint(60, 180))

        self.logger.info(f"Campaign {campaign.name}: Sent {total_sent} DMs total")

        # Update campaign stats
        campaign.total_sent += total_sent
        campaign.save()

    def _send_dm_batch(self, account, users, campaign):
        """Send DMs to a batch of users using one account"""
        cl = setup_client(account)
        if not cl:
            self.logger.error(f"Failed to setup client for {account.username}")
            return 0

        sent_count = 0
        account.status = "sending_dms"
        account.save()

        try:
            for user in users:
                if not account.can_send_dm():
                    self.logger.info(f"Daily DM limit reached for {account.username}")
                    break

                # Generate personalized message
                message = self._generate_personalized_message(user, campaign.template)
                if not message:
                    continue

                # Send DM
                if self._send_single_dm(cl, user, message, account):
                    sent_count += 1
                    account.dms_sent_today += 1
                    account.save()

                    # Update user record
                    user.dm_sent = True
                    user.dm_sent_at = timezone.now()
                    user.dm_account = account
                    user.save()

                    self.logger.info(f"DM sent to {user.username} via {account.username}")
                else:
                    # Stop if DM sending fails consistently
                    break

                # Human-like delay between DMs
                delay = self._calculate_dm_delay(account)
                time.sleep(delay)

        except Exception as e:
            self.logger.error(f"Error in DM batch for {account.username}: {e}")
            account.update_health_score(False, 'dm')

        finally:
            account.status = "idle"
            account.save()

        return sent_count

    def _generate_personalized_message(self, user, template):
        """Generate personalized DM message using AI"""
        try:
            # Use OpenAI to personalize the message
            if hasattr(settings, 'OPENAI_API_KEY') and settings.OPENAI_API_KEY:
                return self._ai_personalize_message(user, template)
            else:
                # Fallback to template variable replacement
                return self._template_personalize_message(user, template)

        except Exception as e:
            self.logger.error(f"Failed to generate personalized message for {user.username}: {e}")
            return template.template  # Fallback to original template

    def _ai_personalize_message(self, user, template):
        """Use AI to personalize the message"""
        try:
            client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

            prompt = f"""
            Personalize this DM template for an Instagram user:

            Template: {template.template}

            User Information:
            - Username: {user.username}
            - Bio: {user.biography[:200]}
            - Profession: {user.profession}
            - Country: {user.country}
            - Followers: {user.follower_count}

            Instructions:
            1. Keep the core message and intent of the template
            2. Make it feel personal and relevant to this specific user
            3. Keep it under 150 characters
            4. Don't be overly familiar or pushy
            5. Make it sound natural and human

            Return only the personalized message, no explanations.
            """

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.7
            )

            personalized = response.choices[0].message.content.strip()

            # Validate length
            if len(personalized) > 200:
                personalized = personalized[:197] + "..."

            return personalized

        except Exception as e:
            self.logger.error(f"AI personalization failed: {e}")
            return self._template_personalize_message(user, template)

    def _template_personalize_message(self, user, template):
        """Simple template-based personalization"""
        message = template.template

        # Replace common placeholders
        replacements = {
            '{username}': user.username,
            '{profession}': user.profession or 'creator',
            '{country}': user.country or 'there',
            '{name}': user.username.replace('_', ' ').replace('.', ' ').title(),
        }

        for placeholder, replacement in replacements.items():
            message = message.replace(placeholder, replacement)

        return message

    def _send_single_dm(self, cl, user, message, account):
        """Send a single DM to a user"""
        try:
            # Get user ID
            user_id = user.user_id
            if not user_id:
                user_id = cl.user_id_from_username(user.username)

            # Send the DM
            cl.direct_send(message, [user_id])

            account.update_health_score(True, 'dm')
            return True

        except ClientError as e:
            error_str = str(e).lower()

            if "429" in error_str:
                account.status = "rate_limited"
                account.save()
                send_alert(f"DM rate limit hit for {account.username}", 'warning', account)
            elif "spam" in error_str:
                account.health_score = max(0, account.health_score - 20)
                account.save()
                send_alert(f"DM marked as spam for {account.username}", 'error', account)
            elif "block" in error_str:
                account.status = "flagged"
                account.save()
                send_alert(f"DM blocked for {account.username}", 'error', account)

            account.update_health_score(False, 'dm')
            self.logger.error(f"Failed to send DM to {user.username}: {e}")
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error sending DM to {user.username}: {e}")
            account.update_health_score(False, 'dm')
            return False

    def _calculate_dm_delay(self, account):
        """Calculate delay between DMs based on account health and age"""
        base_delay = 300  # 5 minutes base

        # Adjust based on account health
        health_factor = (100 - account.health_score) / 100
        health_delay = base_delay * health_factor

        # Adjust based on account age
        if account.account_age_days < 30:
            age_multiplier = 2.0
        elif account.account_age_days < 90:
            age_multiplier = 1.5
        else:
            age_multiplier = 1.0

        # Calculate final delay with randomization
        total_delay = (base_delay + health_delay) * age_multiplier
        randomized_delay = random.uniform(total_delay * 0.7, total_delay * 1.3)

        return int(randomized_delay)
