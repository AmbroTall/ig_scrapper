import os
from pathlib import Path
from celery.schedules import crontab

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = os.getenv('SECRET_KEY', 'django-insecure-k^72qv$!we4r!xyk9o(4we#fj&uz^8gzm4$ppy%6lfrf4^q3*!')

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.getenv('DEBUG', 'True') == 'True'

ALLOWED_HOSTS = os.getenv('ALLOWED_HOSTS', 'localhost,127.0.0.1').split(',')

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    "django_extensions",
    'dmbot'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'app.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [TEMPLATE_DIR],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'app.wsgi.application'

# Database - Use PostgreSQL when DATABASE_URL is set, otherwise SQLite
import dj_database_url

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    DATABASES = {
        'default': dj_database_url.config(
            default=DATABASE_URL,
            conn_max_age=600,
            conn_health_checks=True,
        )
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': BASE_DIR / 'db.sqlite3',
        }
    }

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Static files
STATIC_URL = os.getenv('STATIC_URL', '/static/')
MEDIA_URL = os.getenv('MEDIA_URL', '/files/')
MEDIA_ROOT = os.path.join(BASE_DIR, 'mediafiles')
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Celery settings
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

# Instagram Automation Settings
SCRAPING_LIMIT_PER_SOURCE = int(os.getenv('SCRAPING_LIMIT_PER_SOURCE', 1000))
MAX_INACTIVE_DAYS = int(os.getenv('MAX_INACTIVE_DAYS', 30))
DELAY_RANGE = [
    int(os.getenv('DELAY_RANGE_MIN', 10)),
    int(os.getenv('DELAY_RANGE_MAX', 20))
]
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# DM Settings
MAX_DMS_PER_ACCOUNT_DAILY = int(os.getenv('MAX_DMS_PER_ACCOUNT_DAILY', 15))
DM_DELAY_BASE = int(os.getenv('DM_DELAY_BASE', 300))
DM_PERSONALIZATION_AI = os.getenv('DM_PERSONALIZATION_AI', 'True') == 'True'
SCRAPING_BATCH_SIZE = int(os.getenv('SCRAPING_BATCH_SIZE', 100))

# API Keys
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')

# Celery Beat Schedule
CELERY_BEAT_SCHEDULE = {
    'health-check': {
        'task': 'dmbot.tasks.health_check_task',
        'schedule': crontab(hour='0,5,10,15,20', minute=0),
    },
    'warmup-accounts': {
        'task': 'dmbot.tasks.warmup_accounts_task',
        'schedule': crontab(hour='0,12', minute=0),
    },
    'clean-alerts': {
        'task': 'dmbot.tasks.clean_alerts_task',
        'schedule': 3600,
    },
}