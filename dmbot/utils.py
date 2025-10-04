import logging
import os
import random
import time
import uuid
from datetime import timedelta
from django.utils import timezone
from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired, ChallengeRequired
from django.conf import settings
from .models import Alert
from instagrapi.mixins.challenge import ChallengeChoice

def get_code_from_sms(username):
    # Placeholder: Not used since challenges stop scraping
    return None

def get_code_from_email(username):
    # Placeholder: Not used since challenges stop scraping
    return None

def challenge_code_handler(username, choice):
    # Placeholder: Not used since challenges stop scraping
    return False

def change_password_handler(username):
    # Placeholder: Not used since challenges stop scraping
    return None

def setup_client(account, retry_count=3):
    """Enhanced client setup with better session management for 2000+ accounts"""
    logger = logging.getLogger(__name__)

    for attempt in range(retry_count):
        try:
            if attempt > 0:
                time.sleep(random.uniform(10, 30))  # Wait between retries

            # Check if account is in good state
            if account.status == 'banned':
                logger.error(f"Account {account.username} is banned")
                return None

            # Validate credentials
            if not _validate_credentials(account):
                logger.error(f"Invalid credentials for {account.username}")
                send_alert(f"Invalid credentials for {account.username}", 'error', account)
                return None

            cl = Client()
            _configure_client(cl, account)

            # Try to load existing session
            if _load_session(cl, account):
                if _validate_session(cl, account):
                    logger.info(f"Session loaded successfully for {account.username}")
                    return cl
                else:
                    logger.warning(f"Session validation failed for {account.username}")

            # Fresh login required
            if _perform_login(cl, account):
                _save_session(cl, account)
                account.update_health_score(True, 'login')
                return cl
            else:
                account.update_health_score(False, 'login')

        except Exception as e:
            logger.error(f"Setup attempt {attempt + 1} failed for {account.username}: {e}")
            if attempt == retry_count - 1:
                send_alert(f"Client setup failed for {account.username}: {str(e)}", 'error', account)

    return None


def _validate_credentials(account):
    """Validate account credentials format"""
    if not isinstance(account.username, str) or not isinstance(account.password, str):
        return False
    if "@" in account.username or ".com" in account.username:
        return False
    if len(account.username) < 3 or len(account.password) < 6:
        return False
    return True


def _configure_client(cl, account):
    """Configure client with dynamic device settings for large-scale management"""
    cl.delay_range = getattr(settings, 'DELAY_RANGE', [2, 7])  # Slightly wider for human-like behavior

    if account.device_settings:
        device_settings = account.device_settings
    else:
        # Randomized device profile pool
        android_versions = {
            26: "8.0.0",
            27: "8.1.0",
            28: "9",
            29: "10",
            30: "11",
            31: "12",
        }

        manufacturers_models = {
            "Samsung": ["Galaxy S9", "Galaxy S10", "Galaxy S20", "Galaxy Note 10", "Galaxy A51"],
            "Google": ["Pixel 3", "Pixel 4", "Pixel 5", "Pixel 6"],
            "OnePlus": ["OnePlus 6T", "OnePlus 7T", "OnePlus 8", "OnePlus 9"],
            "Xiaomi": ["Mi 9", "Mi 10", "Redmi Note 8", "Redmi Note 10"],
            "Huawei": ["P30", "Mate 20 Pro", "P40 Lite"],
        }

        manufacturer = random.choice(list(manufacturers_models.keys()))
        model = random.choice(manufacturers_models[manufacturer])
        android_version = random.choice(list(android_versions.keys()))

        device_settings = {
            "app_version": random.choice([
                "268.0.0.18.72", "269.0.0.18.75", "270.0.0.22.98", "271.0.0.12.109"
            ]),
            "android_version": android_version,
            "android_release": android_versions[android_version],
            "dpi": random.choice(["420dpi", "480dpi", "560dpi", "640dpi"]),
            "resolution": random.choice([
                "1080x1920", "1080x2340", "1440x3040", "1440x3200"
            ]),
            "manufacturer": manufacturer,
            "model": model,
            "cpu": random.choice(["qcom", "exynos", "kirin", "mediatek"]),
            "version_code": str(random.randint(300000000, 350000000)),  # Keep changing
            "uuid": str(uuid.uuid4()),  # unique per device
        }

        account.device_settings = device_settings
        account.save(update_fields=["device_settings"])

    # Configure client
    cl.set_device(device_settings)

    # Dynamic user agent
    user_agent = (
        f"Instagram {device_settings['app_version']} "
        f"Android ({device_settings['android_version']}/{device_settings['android_release']}; "
        f"{device_settings['dpi']}; {device_settings['resolution']}; "
        f"{device_settings['manufacturer']}; {device_settings['model']}; "
        f"{device_settings['cpu']})"
    )
    cl.set_user_agent(user_agent)


def _load_session(cl, account):
    """Load existing session if valid, stop on ChallengeRequired"""
    session_file = f"sessions/{account.username}.json"

    if not os.path.exists(session_file):
        return False

    try:
        # Ensure session file is accessible
        if not os.access(session_file, os.R_OK | os.W_OK):
            logging.error(f"Session file {session_file} is not accessible")
            return False

        # Load session
        session = cl.load_settings(session_file)
        if not session:
            return False

        cl.set_settings(session)
        cl.login(account.username, account.password)

        # Validate session
        cl.get_timeline_feed()
        cl.dump_settings(session_file)
        return True

    except ChallengeRequired as ce:
        logging.warning(f"Challenge required for {account.username}: {ce}")
        account.status = "flagged"
        account.save()
        send_alert(f"Challenge required for {account.username}, stopping scraping", 'error', account)
        return False

    except LoginRequired:
        logging.info(f"Session invalid for {account.username}, retrying")
        old_session = cl.get_settings()
        cl.set_settings({})
        if "uuids" in old_session:
            cl.set_uuids(old_session["uuids"])
        if _perform_login(cl, account):
            cl.dump_settings(session_file)
            return True
        return False

    except Exception as e:
        logging.warning(f"Couldn't load session for {account.username}: {e}")
        return False


def _validate_session(cl, account):
    """Validate if session is still working"""
    try:
        cl.get_timeline_feed()
        return True
    except Exception as e:
        logging.warning(f"Session validation error for {account.username}: {e}")
        return False


def _perform_login(cl, account):
    """Perform fresh login with TOTP 2FA support"""
    logger = logging.getLogger(__name__)
    login_success = False

    try:
        logger.info(f"Attempting login for {account.username}")

        if account.secret_key:
            # Clean secret key (remove spaces, uppercase)
            secret_key = account.secret_key.replace(" ", "").upper()
            two_factor_code = cl.totp_generate_code(secret_key)

            # Try login with verification code
            cl.login(
                account.username,
                account.password,
                verification_code=two_factor_code
            )
        else:
            # Normal login if no 2FA configured
            cl.login(account.username, account.password)

        login_success = True

    except Exception as e:
        logger.error(f"Login error for {account.username}: {e}")
        login_success = False

    # Update account status
    if login_success:
        account.last_login = timezone.now()
        account.login_failures = 0
        account.last_login_failure = None
        account.save(update_fields=["last_login", "login_failures", "last_login_failure"])
        logger.info(f"Login successful for {account.username}")
        return True
    else:
        account.login_failures += 1
        account.last_login_failure = timezone.now()
        account.save(update_fields=["login_failures", "last_login_failure"])
        send_alert(f"Login failed for {account.username}", "error", account)
        return False


def _save_session(cl, account):
    """Save session to file"""
    try:
        os.makedirs("sessions", exist_ok=True)
        session_file = f"sessions/{account.username}.json"
        cl.dump_settings(session_file)
        account.session_data = cl.get_settings()
        account.save()
    except Exception as e:
        logging.error(f"Failed to save session for {account.username}: {e}")
        send_alert(f"Failed to save session for {account.username}: {str(e)}", 'error', account)

def send_alert(message, severity='info', account=None):
    """Enhanced alert system with severity levels"""
    Alert.objects.create(
        message=message,
        severity=severity,
        account=account
    )
    logger = logging.getLogger(__name__)
    if severity == 'critical':
        logger.critical(f"CRITICAL ALERT: {message}")
    elif severity == 'error':
        logger.error(f"ERROR ALERT: {message}")
    elif severity == 'warning':
        logger.warning(f"WARNING ALERT: {message}")
    else:
        logger.info(f"INFO ALERT: {message}")


