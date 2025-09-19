## 3. Enhanced Utils (utils.py)

import logging
import os
import random
import time
from datetime import timedelta
from django.utils import timezone
from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired, ChallengeRequired
from django.conf import settings
from .models import Alert
from instagrapi.mixins.challenge import ChallengeChoice

def get_code_from_sms(username):
    # In production, you don’t want input() here – you’d fetch from Twilio, etc.
    while True:
        code = input(f"Enter code (6 digits) for {username}: ").strip()
        if code and code.isdigit():
            return code
    return None

def get_code_from_email(username):
    # Example Gmail IMAP fetch, simplified
    # You’ll replace this with your email API
    return "123456"

def challenge_code_handler(username, choice):
    if choice == ChallengeChoice.SMS:
        return get_code_from_sms(username)
    elif choice == ChallengeChoice.EMAIL:
        return get_code_from_email(username)
    return False

def change_password_handler(username):
    # Generate a new random password if Instagram forces reset
    import random
    chars = list("abcdefghijklmnopqrstuvwxyz1234567890!&£@#")
    return "".join(random.sample(chars, 10))


def setup_client(account, retry_count=3):
    """Enhanced client setup with better session management"""
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
            print("Error",e)
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
    """Configure client with device settings"""
    cl.delay_range = getattr(settings, 'DELAY_RANGE', [1, 3])

    # Use stored device settings or generate new ones
    if account.device_settings:
        device_settings = account.device_settings
    else:
        device_settings = {
            "app_version": "269.0.0.18.75",
            "android_version": random.choice([26, 28, 29, 30]),
            "android_release": random.choice(["8.0.0", "9", "10", "11"]),
            "dpi": random.choice(["420dpi", "480dpi", "560dpi"]),
            "resolution": random.choice(["1080x1920", "1080x2340", "1440x3040"]),
            "manufacturer": random.choice(["Samsung", "Google", "OnePlus", "Xiaomi"]),
            "model": random.choice(["Galaxy S10", "Pixel 5", "OnePlus 8", "Mi 10"]),
            "cpu": "qcom",
            "version_code": "314665256",
        }
        account.device_settings = device_settings
        account.save()

    cl.set_device(device_settings)

    # Set realistic user agent
    user_agents = [
        f"Instagram {device_settings['app_version']} Android ({device_settings['android_version']}/{device_settings['android_release']}; {device_settings['dpi']}; {device_settings['resolution']}; {device_settings['manufacturer']}; {device_settings['model']}; {device_settings['cpu']})",
    ]
    cl.set_user_agent(random.choice(user_agents))


def _load_session(cl, account):
    """Load existing session if valid"""
    session_file = f"sessions/{account.username}.json"

    if not os.path.exists(session_file):
        return False

    try:
        # Load session
        session = cl.load_settings(session_file)
        if not session:
            return False

        cl.set_settings(session)
        cl.login(account.username, account.password)

        # Validate session by hitting timeline
        try:
            cl.get_timeline_feed()
            cl.dump_settings(session_file)
            return True

        except LoginRequired:
            logging.info(f"Session invalid for {account.username}, retrying with password")

            old_session = cl.get_settings()
            cl.set_settings({})
            if "uuids" in old_session:
                cl.set_uuids(old_session["uuids"])

            cl.login(account.username, account.password)
            cl.dump_settings(session_file)
            return True

    except ChallengeRequired as ce:
        logging.warning(f"Challenge required for {account.username}: {ce}")

        # Try to trigger challenge handler
        try:
            cl.challenge_code_handler = challenge_code_handler
            cl.change_password_handler = change_password_handler

            cl.login(account.username, account.password)
            cl.dump_settings(session_file)
            logging.info(f"Challenge solved and session saved for {account.username}")
            return True
        except Exception as e:
            logging.error(f"Challenge handling failed for {account.username}: {e}")
            return False

    except Exception as e:
        logging.warning(f"Couldn't load session for {account.username}: {e}")
        return False

def _validate_session(cl, account):
    """Validate if session is still working"""
    try:
        # Try a simple API call to confirm session is valid
        try:
            cl.get_timeline_feed()
            return True
        except LoginRequired:
            logging.info(f"Session invalid for {account.username}")
            return False
    except Exception as e:
        logging.warning(f"Session validation error for {account.username}: {e}")
        return False

def _perform_login(cl, account):
    """Perform fresh login if session is unavailable/invalid"""
    logger = logging.getLogger(__name__)
    login_success = False

    try:
        logger.info(f"Attempting fresh login for {account.username}")
        if cl.login(account.username, account.password):
            login_success = True
    except Exception as e:
        logger.error(f"Couldn't login {account.username} using username/password: {e}")
        login_success = False

    if login_success:
        # Update login tracking
        account.last_login = timezone.now()
        account.login_failures = 0
        account.last_login_failure = None
        account.save()
        logger.info(f"Login successful for {account.username}")
        return True
    else:
        account.login_failures += 1
        account.last_login_failure = timezone.now()
        account.save()
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


def send_alert(message, severity='info', account=None):
    """Enhanced alert system with severity levels"""
    from .models import Alert

    Alert.objects.create(
        message=message,
        severity=severity,
        account=account
    )

    # Log based on severity
    logger = logging.getLogger(__name__)
    if severity == 'critical':
        logger.critical(f"CRITICAL ALERT: {message}")
    elif severity == 'error':
        logger.error(f"ERROR ALERT: {message}")
    elif severity == 'warning':
        logger.warning(f"WARNING ALERT: {message}")
    else:
        logger.info(f"INFO ALERT: {message}")