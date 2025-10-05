import logging
import os
import random
import time
import uuid
from datetime import timedelta

from django.db import transaction
from django.utils import timezone
from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired, ChallengeRequired
from django.conf import settings
from .models import Alert, Account
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


def setup_client(account, retry_count=2):
    """Enhanced client setup with better session management for 2000+ accounts"""
    logger = logging.getLogger(__name__)

    for attempt in range(retry_count):
        try:
            if attempt > 0:
                time.sleep(random.uniform(10, 30))  # Wait between retries

            # Check if account is in good state
            if account.status == 'banned':
                logger.error(f"Account {account.username} is banned")
                send_alert(f"Account {account.username} is banned", 'error', account)
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
                    send_alert(f"Session loaded successfully for {account.username}", 'info', account)
                    return cl
                else:
                    logger.warning(f"Session validation failed for {account.username}")
                    send_alert(f"Session validation failed for {account.username}", 'warning', account)

            # Fresh login required
            if _perform_login(cl, account):
                _save_session(cl, account)
                with transaction.atomic():
                    account = Account.objects.select_for_update().get(id=account.id)
                    account.update_health_score(True, 'login')
                    account.save()
                return cl
            else:
                with transaction.atomic():
                    account = Account.objects.select_for_update().get(id=account.id)
                    account.update_health_score(False, 'login')
                    account.save()

        except Exception as e:
            logger.error(f"Setup attempt {attempt + 1} failed for {account.username}: {e}")
            send_alert(f"Setup attempt {attempt + 1} failed for {account.username}: {e}", 'error', account)
            if attempt == retry_count - 1:
                send_alert(f"Client setup failed for {account.username}: {str(e)}", 'error', account)

    # After all retries fail, set account status to flagged
    try:
        with transaction.atomic():
            account = Account.objects.select_for_update().get(id=account.id)
            account.status = "flagged"
            account.save()
        logger.info(f"Account {account.username} flagged after {retry_count} failed setup attempts")
        send_alert(f"Account {account.username} flagged after {retry_count} failed setup attempts", 'info', account)
    except Exception as e:
        logger.error(f"Failed to flag account {account.username}: {e}")
        send_alert(f"Failed to flag account {account.username}: {e}", 'error', account)

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
        # Expanded Android versions
        android_versions = {
            26: "8.0.0",
            27: "8.1.0",
            28: "9",
            29: "10",
            30: "11",
            31: "12",
            32: "12L",
            33: "13",
            34: "14",
        }

        # Significantly expanded device pool
        manufacturers_models = {
            "Samsung": [
                "Galaxy S9", "Galaxy S9+", "Galaxy S10", "Galaxy S10+", "Galaxy S10e",
                "Galaxy S20", "Galaxy S20+", "Galaxy S20 Ultra", "Galaxy S21", "Galaxy S21+",
                "Galaxy S21 Ultra", "Galaxy S22", "Galaxy S22+", "Galaxy S22 Ultra",
                "Galaxy S23", "Galaxy S23+", "Galaxy S23 Ultra",
                "Galaxy Note 9", "Galaxy Note 10", "Galaxy Note 10+", "Galaxy Note 20",
                "Galaxy Note 20 Ultra", "Galaxy A51", "Galaxy A52", "Galaxy A53",
                "Galaxy A71", "Galaxy A72", "Galaxy A73", "Galaxy M51", "Galaxy M52",
                "Galaxy Z Fold 3", "Galaxy Z Flip 3", "Galaxy Z Fold 4"
            ],
            "Google": [
                "Pixel 3", "Pixel 3 XL", "Pixel 3a", "Pixel 3a XL",
                "Pixel 4", "Pixel 4 XL", "Pixel 4a", "Pixel 4a 5G",
                "Pixel 5", "Pixel 5a", "Pixel 6", "Pixel 6 Pro",
                "Pixel 6a", "Pixel 7", "Pixel 7 Pro", "Pixel 7a",
                "Pixel 8", "Pixel 8 Pro"
            ],
            "OnePlus": [
                "OnePlus 6", "OnePlus 6T", "OnePlus 7", "OnePlus 7 Pro", "OnePlus 7T",
                "OnePlus 7T Pro", "OnePlus 8", "OnePlus 8 Pro", "OnePlus 8T",
                "OnePlus 9", "OnePlus 9 Pro", "OnePlus 9R", "OnePlus 10 Pro",
                "OnePlus 10T", "OnePlus 11", "OnePlus Nord", "OnePlus Nord 2",
                "OnePlus Nord CE", "OnePlus Nord CE 2"
            ],
            "Xiaomi": [
                "Mi 9", "Mi 9T", "Mi 10", "Mi 10 Pro", "Mi 11", "Mi 11 Pro",
                "Mi 11 Ultra", "Mi 12", "Mi 12 Pro", "Redmi Note 8", "Redmi Note 8 Pro",
                "Redmi Note 9", "Redmi Note 9 Pro", "Redmi Note 10", "Redmi Note 10 Pro",
                "Redmi Note 11", "Redmi Note 11 Pro", "Redmi Note 12", "Redmi K20",
                "Redmi K30", "Redmi K40", "Poco X3", "Poco X4", "Poco F3", "Poco F4"
            ],
            "Oppo": [
                "Find X2", "Find X2 Pro", "Find X3", "Find X3 Pro", "Find X5",
                "Find X5 Pro", "Reno 4", "Reno 5", "Reno 6", "Reno 7", "Reno 8",
                "A74", "A94", "A96"
            ],
            "Vivo": [
                "V20", "V21", "V23", "V25", "V27", "X60", "X60 Pro", "X70",
                "X70 Pro", "X80", "X90", "Y20", "Y33", "Y53"
            ],
            "Realme": [
                "Realme 7", "Realme 8", "Realme 9", "Realme 10", "Realme GT",
                "Realme GT 2", "Realme GT Neo", "Realme GT Neo 2", "Realme GT Neo 3",
                "Realme C25", "Realme C35", "Realme Narzo 30", "Realme Narzo 50"
            ],
            "Motorola": [
                "Moto G8", "Moto G9", "Moto G10", "Moto G20", "Moto G30",
                "Moto G50", "Moto G60", "Moto G100", "Moto Edge 20", "Moto Edge 30",
                "Moto Edge 40", "Motorola One Fusion"
            ],
            "Nokia": [
                "Nokia 7.2", "Nokia 8.3", "Nokia X10", "Nokia X20", "Nokia G20",
                "Nokia G50", "Nokia XR20"
            ],
            "Sony": [
                "Xperia 1", "Xperia 1 II", "Xperia 1 III", "Xperia 1 IV",
                "Xperia 5", "Xperia 5 II", "Xperia 5 III", "Xperia 10 III",
                "Xperia 10 IV"
            ],
            "Asus": [
                "ZenFone 7", "ZenFone 8", "ZenFone 9", "ROG Phone 3", "ROG Phone 5",
                "ROG Phone 6"
            ],
            "LG": [
                "LG G8", "LG V50", "LG V60", "LG Velvet", "LG Wing"
            ],
            "Huawei": [
                "P30", "P30 Pro", "P40", "P40 Pro", "P40 Lite", "P50",
                "Mate 20 Pro", "Mate 30", "Mate 40", "Nova 7", "Nova 8", "Nova 9"
            ],
        }

        manufacturer = random.choice(list(manufacturers_models.keys()))
        model = random.choice(manufacturers_models[manufacturer])
        android_version = random.choice(list(android_versions.keys()))

        device_settings = {
            "app_version": random.choice([
                "268.0.0.18.72", "269.0.0.18.75", "270.0.0.22.98", "271.0.0.12.109",
                "272.0.0.16.71", "273.0.0.21.70", "274.0.0.18.75"
            ]),
            "android_version": android_version,
            "android_release": android_versions[android_version],
            "dpi": random.choice(["420dpi", "480dpi", "520dpi", "560dpi", "640dpi"]),
            "resolution": random.choice([
                "1080x1920", "1080x2160", "1080x2280", "1080x2340", "1080x2400",
                "1440x2560", "1440x3040", "1440x3200", "1440x3120"
            ]),
            "manufacturer": manufacturer,
            "model": model,
            "cpu": random.choice(["qcom", "exynos", "kirin", "mediatek", "unisoc"]),
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
            send_alert(f"Session file {session_file} is not accessible", 'error', account)
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
        send_alert(f"Challenge required for {account.username}: {ce}", 'warning', account)
        account.status = "flagged"
        account.save()
        send_alert(f"Challenge required for {account.username}, stopping scraping", 'error', account)
        return False

    except LoginRequired:
        logging.info(f"Session invalid for {account.username}, retrying")
        send_alert(f"Session invalid for {account.username}, retrying", 'info', account)
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
        send_alert(f"Couldn't load session for {account.username}: {e}", 'warning', account)
        return False


def _validate_session(cl, account):
    """Validate if session is still working"""
    try:
        cl.get_timeline_feed()
        return True
    except Exception as e:
        logging.warning(f"Session validation error for {account.username}: {e}")
        send_alert(f"Session validation error for {account.username}: {e}", 'warning', account)
        return False


def _perform_login(cl, account):
    """Perform fresh login with TOTP 2FA support"""
    logger = logging.getLogger(__name__)
    login_success = False

    try:
        logger.info(f"Attempting login for {account.username}")
        send_alert(f"Attempting login for {account.username}", 'info', account)

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
        send_alert(f"Login error for {account.username}: {e}", 'error', account)
        login_success = False

    # Update account status
    if login_success:
        account.last_login = timezone.now()
        account.login_failures = 0
        account.last_login_failure = None
        account.save(update_fields=["last_login", "login_failures", "last_login_failure"])
        logger.info(f"Login successful for {account.username}")
        send_alert(f"Login successful for {account.username}", 'info', account)
        return True
    else:
        account.login_failures += 1
        account.last_login_failure = timezone.now()
        account.save(update_fields=["login_failures", "last_login_failure"])
        send_alert(f"Login failed for {account.username}", 'error', account)
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
        send_alert(f"Failed to save session for {account.username}: {e}", 'error', account)


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