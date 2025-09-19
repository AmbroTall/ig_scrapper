# Enhanced Instagram Automation System

## 1. Improved Models (models.py)

from django.db import models
from django.utils import timezone
from django.core.validators import MinValueValidator, MaxValueValidator
import json


class Account(models.Model):
    username = models.CharField(max_length=255, unique=True)
    password = models.CharField(max_length=255)
    session_data = models.JSONField(null=True, blank=True)
    device_settings = models.JSONField(null=True, blank=True)

    # Status and Health Monitoring
    status = models.CharField(
        max_length=50,
        choices=[
            ("idle", "Idle"),
            ("scraping", "Scraping"),
            ("sending_dms", "Sending DMs"),
            ("rate_limited", "Rate Limited"),
            ("error", "Error"),
            ("warming_up", "Warming Up"),
            ("banned", "Banned"),
            ("flagged", "Flagged"),
        ],
        default="idle",
    )
    health_score = models.IntegerField(
        default=100,
        validators=[MinValueValidator(0), MaxValueValidator(100)]
    )

    # Login Management
    last_login = models.DateTimeField(null=True, blank=True)
    login_failures = models.IntegerField(default=0)
    last_login_failure = models.DateTimeField(null=True, blank=True)

    # Activity Tracking
    last_active = models.DateTimeField(default=timezone.now)
    users_scraped_today = models.IntegerField(default=0)
    dms_sent_today = models.IntegerField(default=0)
    last_reset = models.DateField(default=timezone.now)

    # Limits and Configuration
    daily_scrape_limit = models.IntegerField(default=50)
    daily_dm_limit = models.IntegerField(default=15)
    account_age_days = models.IntegerField(default=30)

    # Warm-up Status
    warmed_up = models.BooleanField(default=False)
    warmup_level = models.IntegerField(default=0)  # 0-5 levels

    # Rate Limiting
    last_action_time = models.DateTimeField(null=True, blank=True)
    actions_this_hour = models.IntegerField(default=0)
    hour_reset = models.DateTimeField(default=timezone.now)

    def reset_daily_counters(self):
        """Reset daily counters if it's a new day"""
        today = timezone.now().date()
        if self.last_reset < today:
            self.users_scraped_today = 0
            self.dms_sent_today = 0
            self.last_reset = today
            self.save()

    def can_scrape(self):
        self.reset_daily_counters()
        return (
                self.users_scraped_today < self.daily_scrape_limit and
                self.status not in ['banned', 'rate_limited', 'error'] and
                self.health_score > 30
        )

    def can_send_dm(self):
        self.reset_daily_counters()
        return (
                self.dms_sent_today < self.daily_dm_limit and
                self.status not in ['banned', 'rate_limited', 'error'] and
                self.health_score > 50 and
                self.warmed_up
        )

    def update_health_score(self, action_success=True, action_type='general'):
        """Update account health based on action success"""
        if action_success:
            if self.health_score < 100:
                self.health_score = min(100, self.health_score + 2)
        else:
            penalty = {'login': 15, 'dm': 10, 'scrape': 5}.get(action_type, 5)
            self.health_score = max(0, self.health_score - penalty)

            if self.health_score < 20:
                self.status = 'flagged'
            elif self.health_score < 50:
                self.status = 'error'

        self.save()

    def __str__(self):
        return f"{self.username} (Health: {self.health_score}%)"


class ScrapedUser(models.Model):
    username = models.CharField(max_length=255, unique=True)
    user_id = models.CharField(max_length=255, unique=True, null=True, blank=True)
    biography = models.TextField(blank=True)
    follower_count = models.IntegerField(default=0)
    following_count = models.IntegerField(default=0)
    post_count = models.IntegerField(default=0)

    # User details
    failure_reason = models.TextField(blank=True, null=True)
    # Activity Analysis
    last_post_date = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    engagement_rate = models.FloatField(default=0.0)

    # Classification
    country = models.CharField(max_length=100, blank=True)
    profession = models.CharField(max_length=100, blank=True)
    keywords = models.JSONField(default=list)
    category_confidence = models.FloatField(default=0.0)

    # Metadata
    scraped_at = models.DateTimeField(default=timezone.now)
    account = models.ForeignKey(Account, on_delete=models.CASCADE)
    source_type = models.CharField(max_length=50)  # hashtag, location, etc.
    source_value = models.CharField(max_length=255)  # actual hashtag/location

    # DM Status
    dm_sent = models.BooleanField(default=False)
    dm_sent_at = models.DateTimeField(null=True, blank=True)
    dm_account = models.ForeignKey(
        Account, on_delete=models.SET_NULL,
        null=True, blank=True, related_name='sent_dms'
    )
    dm_response_received = models.BooleanField(default=False)

    class Meta:
        indexes = [
            models.Index(fields=['is_active', 'dm_sent']),
            models.Index(fields=['profession', 'country']),
            models.Index(fields=['scraped_at']),
        ]

    def __str__(self):
        return f"{self.username} ({self.profession}, {self.country})"


class DMTemplate(models.Model):
    name = models.CharField(max_length=100)
    template = models.TextField()
    category = models.CharField(max_length=50, default='general')
    success_rate = models.FloatField(default=0.0)
    times_used = models.IntegerField(default=0)
    active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.name} ({self.success_rate:.1%} success)"


class DMCampaign(models.Model):
    name = models.CharField(max_length=100)
    template = models.ForeignKey(DMTemplate, on_delete=models.CASCADE)
    target_filters = models.JSONField(default=dict)  # profession, country, keywords
    accounts = models.ManyToManyField(Account, related_name='campaigns')

    # Campaign Stats
    total_sent = models.IntegerField(default=0)
    responses_received = models.IntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    is_active = models.BooleanField(default=True)

    @property
    def response_rate(self):
        return self.responses_received / self.total_sent if self.total_sent > 0 else 0

    def __str__(self):
        return f"{self.name} ({self.response_rate:.1%} response)"


class Alert(models.Model):
    SEVERITY_CHOICES = [
        ('info', 'Info'),
        ('warning', 'Warning'),
        ('error', 'Error'),
        ('critical', 'Critical'),
    ]

    message = models.TextField()
    severity = models.CharField(max_length=20, choices=SEVERITY_CHOICES, default='info')
    account = models.ForeignKey(Account, on_delete=models.CASCADE, null=True, blank=True)
    timestamp = models.DateTimeField(default=timezone.now)
    acknowledged = models.BooleanField(default=False)

    class Meta:
        ordering = ['-timestamp']

    def __str__(self):
        return f"[{self.severity.upper()}] {self.message[:50]}..."
