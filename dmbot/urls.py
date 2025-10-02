from django.urls import path
from . import views


urlpatterns = [
    path('input/', views.InputFormView.as_view(), name='input_form'),
    path('status/', views.StatusView.as_view(), name='status'),
    path('accounts/', views.AccountUploadView.as_view(), name='account_upload'),
    path('alerts/<int:alert_id>/acknowledge/', views.AlertAcknowledgeView.as_view(), name='alert_acknowledge'),
    path('campaign/', views.DMCampaignView.as_view(), name='campaign'),
    path('templates/', views.DMTemplateView.as_view(), name='template_form'),
    path('scraped-users/', views.ScrapedUsersView.as_view(), name='scraped_users'),
]
