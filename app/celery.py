# dmbot/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")

app = Celery("dmbot")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()