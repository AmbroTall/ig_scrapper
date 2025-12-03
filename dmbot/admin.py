from django.contrib import admin
from .models import (
    Account, ScrapedUser, DMTemplate, DMCampaign,
    Alert, DMLog, ProcessedMedia, ScheduledTask, DailyMetric
)


@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = (
        "username", "status", "health_score", "warmed_up",
        "daily_scrape_limit", "daily_dm_limit",
        "users_scraped_today", "dms_sent_today",
        "last_active",
    )
    list_filter = ("status", "warmed_up", "last_reset")
    search_fields = ("username",)
    readonly_fields = ("last_active", "last_login", "last_login_failure", "last_reset")
    ordering = ("-health_score",)

    fieldsets = (
        ("Account Info", {
            "fields": ("username", "password", "secret_key", "session_data", "device_settings")
        }),
        ("Status & Health", {
            "fields": ("status", "health_score", "last_active")
        }),
        ("Login", {
            "fields": ("last_login", "login_failures", "last_login_failure")
        }),
        ("Activity Tracking", {
            "fields": ("users_scraped_today", "dms_sent_today", "last_reset")
        }),
        ("Limits & Config", {
            "fields": ("daily_scrape_limit", "daily_dm_limit", "account_age_days")
        }),
        ("Warmup & Rate Limiting", {
            "fields": ("warmed_up", "warmup_level", "last_action_time", "actions_this_hour", "hour_reset", "task_id")
        }),
    )


@admin.register(ScrapedUser)
class ScrapedUserAdmin(admin.ModelAdmin):
    list_display = (
        "username", "profession", "country", "is_active",
        "follower_count", "engagement_rate", "dm_sent", "scraped_at", "details_fetched"
    )
    list_filter = ("is_active", "dm_sent", "details_fetched", "profession", "country", "scraped_at")
    search_fields = ("username", "user_id", "profession", "country", "keywords")
    readonly_fields = ("scraped_at",)
    ordering = ("-scraped_at",)


@admin.register(DMTemplate)
class DMTemplateAdmin(admin.ModelAdmin):
    list_display = ("name", "category", "success_rate", "times_used", "active")
    list_filter = ("category", "active")
    search_fields = ("name", "template")
    ordering = ("-success_rate",)


@admin.register(DMCampaign)
class DMCampaignAdmin(admin.ModelAdmin):
    list_display = ("name", "template", "total_sent", "responses_received", "response_rate", "is_active", "created_at")
    list_filter = ("is_active", "created_at")
    search_fields = ("name", "template__name")
    filter_horizontal = ("accounts",)
    readonly_fields = ("created_at", "total_sent", "responses_received")
    ordering = ("-created_at",)


@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = ("message", "severity", "account", "timestamp", "acknowledged")
    list_filter = ("severity", "acknowledged", "timestamp")
    search_fields = ("message", "account__username")
    ordering = ("-timestamp",)
    readonly_fields = ("timestamp",)


@admin.register(DMLog)
class DMLogAdmin(admin.ModelAdmin):
    list_display = ("sender_account", "recipient_user", "sent_at")
    search_fields = ("sender_account__username", "recipient_user__username", "message")
    list_filter = ("sent_at",)
    ordering = ("-sent_at",)
    readonly_fields = ("sent_at",)


@admin.register(ProcessedMedia)
class ProcessedMediaAdmin(admin.ModelAdmin):
    list_display = ("media_id", "hashtag", "account", "source_type", "processed_at")
    list_filter = ("source_type", "processed_at")
    search_fields = ("media_id", "hashtag", "account__username")
    ordering = ("-processed_at",)
    readonly_fields = ("processed_at",)


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    list_display = ('task_type', 'scheduled_time', 'status', 'created_at', 'parameters_summary')
    list_filter = ('task_type', 'status', 'scheduled_time')
    search_fields = ('task_id', 'parameters__source_value', 'parameters__name')
    date_hierarchy = 'scheduled_time'
    readonly_fields = ('created_at', 'updated_at', 'task_id', 'parameters')
    ordering = ('-scheduled_time',)

    def parameters_summary(self, obj):
        """Display a summary of the task parameters."""
        if obj.task_type == 'scrape':
            return f"{obj.parameters.get('source_type', '').capitalize()}: {obj.parameters.get('source_value', '')}"
        elif obj.task_type == 'enrich':
            return f"Accounts: {', '.join(map(str, obj.parameters.get('account_ids', [])))}"
        elif obj.task_type == 'dm_campaign':
            return f"Campaign: {obj.parameters.get('name', '')}"
        elif obj.task_type == 'dm_csv':
            return f"CSV: {obj.parameters.get('name', '')}"
        return str(obj.parameters)
    parameters_summary.short_description = 'Parameters'

    def get_queryset(self, request):
        """Optimize queryset to reduce database hits."""
        return super().get_queryset(request).select_related()


@admin.register(DailyMetric)
class DailyMetricAdmin(admin.ModelAdmin):
    list_display = (
        'date',
        'scraped_count',
        'enriched_count',
        'dm_sent_count',
        'scraping_threshold',
        'scraping_limit_reached',
    )

    list_filter = ('scraping_limit_reached', 'date',)
    search_fields = ('date',)
    ordering = ('-date',)