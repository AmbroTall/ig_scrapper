from django.urls import path
from . import views


urlpatterns = [
    path('input/', views.InputFormView.as_view(), name='input_form'),
    path('', views.StatusView.as_view(), name='status'),
    path('accounts/', views.AccountUploadView.as_view(), name='account_upload'),
    path('alerts/<int:alert_id>/acknowledge/', views.AlertAcknowledgeView.as_view(), name='alert_acknowledge'),
    path('campaign/', views.DMCampaignView.as_view(), name='campaign'),
    path('templates/', views.DMTemplateView.as_view(), name='template_form'),
    path('scraped-users/', views.ScrapedUsersView.as_view(), name='scraped_users'),
    path('bot-activity/', views.BotActivityView.as_view(), name='bot_activity'),
    path('sent-messages/', views.SentMessagesView.as_view(), name='sent_messages'),
    path('account-management/', views.AccountManagementView.as_view(), name='account_management'),
    path('api/latest-activities/', views.latest_activities_api, name='latest_activities_api'),
    path('api/recent-logs/', views.recent_logs_api, name='recent_logs_api'),
    path('enrichment/', views.EnrichmentFormView.as_view(), name='enrichment_form'),
]
