# dm_bot.py
import argparse
import logging
from django.conf import settings
from django.utils import timezone
from dm_sender import DMSender
from filter import UserFilter
from models import DMCampaign, Account

def run_dm_bot(campaign_id, max_dms_per_account=15, professions=None, countries=None, keywords=None):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    dm_sender = DMSender()
    user_filter = UserFilter()

    try:
        campaign = DMCampaign.objects.get(id=campaign_id)
        if not campaign.is_active:
            logger.info(f"Campaign {campaign.name} is inactive")
            return

        # Apply filters dynamically
        users = user_filter.filter_users(
            professions=professions,
            countries=countries,
            keywords=keywords,
            activity_days=30
        ).filter(account__in=campaign.accounts.all())[:max_dms_per_account * campaign.accounts.count()]

        dm_sender.send_dms_for_campaign(campaign_id, max_dms_per_account)
        logger.info(f"Standalone DM bot completed for campaign {campaign_id}")
    except Exception as e:
        logger.error(f"DM bot failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone Instagram DM Bot")
    parser.add_argument("--campaign_id", type=int, required=True, help="ID of the DM campaign")
    parser.add_argument("--max_dms", type=int, default=15, help="Max DMs per account")
    parser.add_argument("--professions", type=str, help="Comma-separated professions")
    parser.add_argument("--countries", type=str, help="Comma-separated countries")
    parser.add_argument("--keywords", type=str, help="Comma-separated keywords")
    args = parser.parse_args()

    professions = args.professions.split(",") if args.professions else None
    countries = args.countries.split(",") if args.countries else None
    keywords = args.keywords.split(",") if args.keywords else None

    run_dm_bot(args.campaign_id, args.max_dms, professions, countries, keywords)