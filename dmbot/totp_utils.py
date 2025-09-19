import logging
from instagrapi import Client


import pyotp   # <-- for TOTP seed handling (standard library for OTP)

logger = logging.getLogger(__name__)

# ==============================
# TOTP (Two Factor Auth) Helpers
# ==============================

def generate_totp_seed():
    """Generate a new TOTP secret seed"""
    cl = Client()
    return cl.totp_generate_seed()

def get_totp_code(seed: str):
    """Generate a TOTP code from stored seed"""
    cl = Client()
    return cl.totp_generate_code(seed)

def enable_totp(cl: Client, account, verification_code: str):
    """
    Enable TOTP on the account
    verification_code = code from Google Authenticator or cl.totp_generate_code(seed)
    Returns backup keys
    """
    try:
        backup_keys = cl.totp_enable(verification_code)
        account.totp_seed = cl.totp_generate_seed()
        account.totp_backup_keys = backup_keys
        account.save(update_fields=["totp_seed", "totp_backup_keys"])
        return backup_keys
    except Exception as e:
        logger.error(f"Failed to enable TOTP for {account.username}: {e}")
        return []

def disable_totp(cl: Client, account):
    """Disable TOTP"""
    try:
        if cl.totp_disable():
            account.totp_seed = None
            account.totp_backup_keys = None
            account.save(update_fields=["totp_seed", "totp_backup_keys"])
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to disable TOTP for {account.username}: {e}")
        return False
