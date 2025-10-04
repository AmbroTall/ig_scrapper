import re

from celery import Celery
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.contrib import messages
from django.views import View
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .models import Account, Alert, ScrapedUser, DMCampaign, DMTemplate, DMLog
from .tasks import scrape_users_task, send_dms_task, cancel_task
import csv
from io import TextIOWrapper
import random


# Initialize Celery app (adjust this based on your Celery configuration)
app = Celery('app', broker='redis://localhost:6379/0')  # Update with your broker URL

class InputFormView(LoginRequiredMixin, View):
    """View to handle input for scraping parameters"""
    def get(self, request):
        accounts = Account.objects.filter(status='idle', health_score__gte=50)
        return render(request, 'dmbot/input_form.html', {'accounts': accounts})

    def post(self, request):
        try:
            # Extract and validate inputs
            hashtags = [h.strip() for h in request.POST.get('hashtags', '').split(',') if h.strip()]
            locations = [l.strip() for l in request.POST.get('locations', '').split(',') if l.strip()]
            tags = [t.strip() for t in request.POST.get('tags', '').split(',') if t.strip()]
            account_ids = request.POST.getlist('accounts')

            # Validate at least one source is provided
            if not any([hashtags, locations, tags]):
                messages.error(request, "At least one scraping source (e.g., hashtag, location, or tag) is required.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            # Validate at least one account is selected
            if not account_ids:
                messages.error(request, "At least one account must be selected for scraping.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            # Get selected accounts
            accounts = Account.objects.filter(id__in=account_ids, status='idle', health_score__gte=50)
            if not accounts:
                messages.error(request, "No valid accounts selected for scraping.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            # Schedule scraping tasks with randomized delays
            first_account = True
            for account in accounts:
                # Reset daily counters if needed
                account.reset_daily_counters()
                for hashtag in hashtags:
                    if first_account:
                        scrape_users_task.delay(account.id, "hashtag", hashtag)
                        first_account = False
                    else:
                        delay = random.randint(300, 900)  # 5–15 min
                        scrape_users_task.apply_async((account.id, "hashtag", hashtag), countdown=delay)
                for location in locations:
                    delay = random.randint(300, 900)
                    scrape_users_task.apply_async((account.id, "location", location), countdown=delay)
                for tag in tags:
                    delay = random.randint(300, 900)
                    scrape_users_task.apply_async((account.id, "tags", tag), countdown=delay)

            messages.success(request, "Scraping tasks scheduled successfully.")
            return redirect('status')
        except Exception as e:
            messages.error(request, f"Error scheduling tasks: {str(e)}")
            return render(request, 'dmbot/input_form.html', {
                'accounts': Account.objects.filter(status='idle', health_score__gte=50)
            })

class BotActivityView(View):
    def get(self, request):
        search_query = request.GET.get('search', '')

        # Fetch active Celery tasks
        processed_activities = []
        inspector = app.control.inspect()
        try:
            active_tasks = inspector.active() or {}  # Get active tasks from all workers
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    task_id = task.get('id')
                    task_name = task.get('name', 'Unknown Task')
                    args = task.get('args', [])
                    # Extract account username from task args if available
                    username = 'System'
                    try:
                        # Adjust based on your task args structure, e.g., (account_id, campaign_id)
                        if args and isinstance(args, (list, tuple)) and len(args) > 0:
                            account_id = args[0]  # Adjust based on your task args
                            account = Account.objects.filter(id=account_id).first()
                            username = account.username if account else 'System'
                    except Exception:
                        pass  # Fallback to 'System' if parsing fails
                    # Apply search filter
                    if search_query:
                        if not (search_query.lower() in task_name.lower() or
                                search_query.lower() in username.lower()):
                            continue
                    processed_activities.append({
                        'task_id': task_id,
                        'message': f"Running task: {task_name}",
                        'username': username,
                        'timestamp': task.get('time_start', None)  # Unix timestamp from Celery
                    })
        except Exception as e:
            print(f"Error fetching active tasks: {e}")
            processed_activities = []

        # Paginate processed activities
        paginator = Paginator(processed_activities, 10)
        page_number = request.GET.get('page', 1)
        activities_paginated = paginator.get_page(page_number)

        return render(request, 'dmbot/bot_activity.html', {
            'activities': activities_paginated,
            'search_query': search_query
        })

    def post(self, request):
        task_id = request.POST.get('task_id')
        if task_id:
            try:
                # Directly revoke the task
                app.control.revoke(task_id, terminate=True)
                if request.is_ajax():
                    return JsonResponse({'status': 'cancelled', 'message': f'Task {task_id} cancelled'})
                messages.success(request, f"Task {task_id} cancelled successfully.")
            except Exception as e:
                if request.is_ajax():
                    return JsonResponse({'status': 'failed', 'error': str(e)})
                messages.error(request, f"Failed to cancel task: {str(e)}")
        else:
            if request.is_ajax():
                return JsonResponse({'status': 'failed', 'error': 'No task_id provided'})
            messages.error(request, "No task_id provided")
        return redirect('bot_activity')

class StatusView(View):
    def get(self, request):
        # Fetch accounts and campaigns with ordering
        accounts = Account.objects.all().order_by('-last_active')  # Order by last_active descending
        campaigns = DMCampaign.objects.select_related('template').all().order_by('-created_at')  # Order by created_at descending
        templates = DMTemplate.objects.filter(active=True)
        alerts = Alert.objects.filter(severity__in=['error', 'warning', 'critical']).order_by('-timestamp')[:5]
        pending_enrichment = ScrapedUser.objects.filter(details_fetched=False).count()
        enriched_users = ScrapedUser.objects.filter(details_fetched=True).count()

        account_search = request.GET.get('account_search', '')
        campaign_search = request.GET.get('campaign_search', '')

        if account_search:
            accounts = accounts.filter(
                Q(username__icontains=account_search) |
                Q(status__icontains=account_search)
            )
        if campaign_search:
            campaigns = campaigns.filter(
                Q(name__icontains=campaign_search) |
                Q(template__name__icontains=campaign_search)
            )

        # Paginate only if object count > 5
        accounts_paginator = Paginator(accounts, 5) if accounts.count() > 5 else None
        campaigns_paginator = Paginator(campaigns, 5) if campaigns.count() > 5 else None

        accounts_page = request.GET.get('accounts_page', 1)
        campaigns_page = request.GET.get('campaigns_page', 1)

        accounts_paginated = accounts_paginator.get_page(accounts_page) if accounts_paginator else accounts
        campaigns_paginated = campaigns_paginator.get_page(campaigns_page) if campaigns_paginator else campaigns

        return render(request, 'dmbot/status.html', {
            'accounts': accounts_paginated,
            'campaigns': campaigns_paginated,
            'templates': templates,
            'alerts': alerts,
            'pending_enrichment': pending_enrichment,
            'enriched_users': enriched_users,
            'account_search': account_search,
            'campaign_search': campaign_search,
        })

class AccountUploadView(LoginRequiredMixin, View):
    """View to handle CSV upload for Instagram accounts"""
    def get(self, request):
        return render(request, 'dmbot/accounts.html')

    def post(self, request):
        try:
            csv_file = request.FILES.get('csv_file')
            if not csv_file:
                messages.error(request, "No CSV file uploaded.")
                return render(request, 'dmbot/accounts.html')

            csv_reader = csv.reader(TextIOWrapper(csv_file, 'utf-8'))
            next(csv_reader, None)  # Skip header
            created_count = 0
            skipped_count = 0
            for row in csv_reader:
                if len(row) >= 2:
                    username, password, secret_key = row[0].strip(), row[1].strip(), row[2].strip()
                    if not username or len(password) < 6:
                        skipped_count += 1
                        continue
                    Account.objects.get_or_create(
                        username=username,
                        defaults={
                            'password': password,
                            'secret_key': secret_key,
                            'status': 'idle',
                            'last_login': None,
                            'login_failures': 0,
                            'warmed_up': False,
                            'account_age_days': 0,  # Update manually or via external data
                            'daily_scrape_limit': 50,
                            'daily_dm_limit': 15,
                        }
                    )
                    created_count += 1
                else:
                    skipped_count += 1

            messages.success(request, f"Uploaded {created_count} accounts, skipped {skipped_count} invalid rows.")
            return redirect('status')
        except Exception as e:
            messages.error(request, f"Error uploading accounts: {str(e)}")
            return render(request, 'dmbot/accounts.html')

class AlertAcknowledgeView(LoginRequiredMixin, View):
    """View to acknowledge alerts"""
    def post(self, request, alert_id):
        try:
            alert = Alert.objects.get(id=alert_id)
            alert.acknowledged = True
            alert.save()
            messages.success(request, "Alert acknowledged.")
        except Alert.DoesNotExist:
            messages.error(request, "Alert not found.")
        return redirect('status')

class DMCampaignView(LoginRequiredMixin, View):
    def get(self, request):
        templates = DMTemplate.objects.filter(active=True)
        if not templates:
            messages.error(request, "No active DM templates available. Please create a template first.")
            return redirect('template_form')
        accounts = Account.objects.filter(status='idle', warmed_up=True)
        return render(request, 'dmbot/campaign_form.html', {'templates': templates, 'accounts': accounts})

    def post(self, request):
        try:
            name = request.POST.get('name')
            template_id = request.POST.get('template_id')
            account_ids = request.POST.getlist('accounts')
            filters = {
                'professions': [p.strip() for p in request.POST.get('professions', '').split(',') if p.strip()],
                'countries': [c.strip() for c in request.POST.get('countries', '').split(',') if c.strip()],
                'keywords': [k.strip() for k in request.POST.get('keywords', '').split(',') if k.strip()]
            }
            if not DMTemplate.objects.filter(id=template_id, active=True).exists():
                messages.error(request, "Selected template is invalid or inactive.")
                return render(request, 'dmbot/campaign_form.html', {
                    'templates': DMTemplate.objects.filter(active=True),
                    'accounts': Account.objects.filter(status='idle', warmed_up=True)
                })
            campaign = DMCampaign.objects.create(
                name=name, template_id=template_id, target_filters=filters
            )
            campaign.accounts.set(account_ids)
            send_dms_task.delay(campaign.id)
            messages.success(request, f"Campaign {name} started.")
            return redirect('status')
        except Exception as e:
            messages.error(request, f"Error starting campaign: {str(e)}")
            return render(request, 'dmbot/campaign_form.html', {
                'templates': DMTemplate.objects.filter(active=True),
                'accounts': Account.objects.filter(status='idle', warmed_up=True)
            })

class DMTemplateView(LoginRequiredMixin, View):
    """View to create and manage DM templates with pagination and search"""

    def get(self, request):
        # Get search query
        search_query = request.GET.get('search', '').strip()

        # Get all templates
        templates_list = DMTemplate.objects.all().order_by("id")

        # Apply search filter
        if search_query:
            templates_list = templates_list.filter(
                Q(name__icontains=search_query) |
                Q(template__icontains=search_query) |
                Q(category__icontains=search_query)
            )

        # Pagination
        paginator = Paginator(templates_list, 5)  # 10 templates per page
        page = request.GET.get('page', 1)

        try:
            templates = paginator.page(page)
        except PageNotAnInteger:
            templates = paginator.page(1)
        except EmptyPage:
            templates = paginator.page(paginator.num_pages)

        categories = ['general', 'photography', 'art', 'travel', 'business', 'other']

        return render(request, 'dmbot/template_form.html', {
            'templates': templates,
            'categories': categories,
            'search_query': search_query,
        })

    def post(self, request):
        try:
            name = request.POST.get('name')
            template_text = request.POST.get('template')
            category = request.POST.get('category', 'general')

            if not name or not template_text:
                messages.error(request, "Name and template text are required.")
                return redirect('template_form')

            DMTemplate.objects.create(
                name=name,
                template=template_text,
                category=category or 'general',
                active=True
            )
            messages.success(request, f"Template '{name}' created successfully.")
            return redirect('campaign')

        except Exception as e:
            messages.error(request, f"Error creating template: {str(e)}")
            return redirect('template_form')

class ScrapedUsersView(LoginRequiredMixin, View):
    """View to display list of scraped users with pagination and search"""

    def get(self, request):
        # Get search query
        search_query = request.GET.get('search', '').strip()

        users_list = ScrapedUser.objects.select_related('account').all().order_by('-scraped_at')

        # Apply search filter
        if search_query:
            users_list = users_list.filter(
                Q(username__icontains=search_query) |
                Q(biography__icontains=search_query) |
                Q(profession__icontains=search_query) |
                Q(country__icontains=search_query) |
                Q(account__username__icontains=search_query)
            )

        # Pagination
        paginator = Paginator(users_list, 10)  # 20 users per page
        page = request.GET.get('page', 1)

        try:
            users = paginator.page(page)
        except PageNotAnInteger:
            users = paginator.page(1)
        except EmptyPage:
            users = paginator.page(paginator.num_pages)

        return render(request, 'dmbot/scraped_users.html', {
            'users': users,
            'search_query': search_query,
        })

class SentMessagesView(View):
    def get(self, request):
        search_query = request.GET.get('search', '')
        messages_list = DMLog.objects.select_related('sender_account', 'recipient_user').order_by('-sent_at')

        if search_query:
            messages_list = messages_list.filter(
                Q(recipient_user__username__icontains=search_query) |
                Q(message__icontains=search_query) |
                Q(sender_account__username__icontains=search_query)
            )

        paginator = Paginator(messages_list, 10)
        page_number = request.GET.get('page', 1)
        messages_paginated = paginator.get_page(page_number)

        return render(request, 'dmbot/sent_messages.html', {
            'messages': messages_paginated,
            'search_query': search_query
        })

class AccountManagementView(View):
    def get(self, request):
        search_query = request.GET.get('search', '')
        accounts = Account.objects.all().order_by('username')

        if search_query:
            accounts = accounts.filter(
                Q(username__icontains=search_query) |
                Q(status__icontains=search_query)
            )

        paginator = Paginator(accounts, 10)
        page_number = request.GET.get('page', 1)
        accounts_paginated = paginator.get_page(page_number)

        return render(request, 'dmbot/account_management.html', {
            'accounts': accounts_paginated,
            'search_query': search_query
        })

    def post(self, request):
        action = request.POST.get('action')
        account_id = request.POST.get('account_id')

        try:
            account = Account.objects.get(id=account_id)
            if action == 'delete':
                account.delete()
                messages.success(request, f"Account {account.username} deleted successfully.")
            elif action == 'update':
                username = request.POST.get('username')
                password = request.POST.get('password')
                secret_key = request.POST.get('secret_key')
                account.username = username
                account.password = password
                account.secret_key = secret_key
                account.save()
                messages.success(request, f"Account {account.username} updated successfully.")
        except Account.DoesNotExist:
            messages.error(request, "Account not found.")
        except Exception as e:
            messages.error(request, f"Error performing action: {str(e)}")

        return redirect('account_management')

@require_http_methods(["GET"])
def latest_activities_api(request):
    processed_activities = []
    inspector = app.control.inspect()
    try:
        active_tasks = inspector.active() or {}
        for worker, tasks in active_tasks.items():
            for task in tasks:
                task_id = task.get('id')
                task_name = task.get('name', 'Unknown Task')
                args = task.get('args', [])
                username = 'System'
                try:
                    if args and isinstance(args, (list, tuple)) and len(args) > 0:
                        account_id = args[0]  # Adjust based on your task args
                        account = Account.objects.filter(id=account_id).first()
                        username = account.username if account else 'System'
                except Exception:
                    pass
                from datetime import datetime
                timestamp = datetime.fromtimestamp(task.get('time_start', 0)) if task.get('time_start') else None
                processed_activities.append({
                    'task_id': task_id,
                    'message': f"Running task: {task_name}",
                    'username': username,
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M') if timestamp else None
                })
    except Exception as e:
        print(f"Error fetching active tasks: {e}")
    return JsonResponse({'activities': processed_activities})

@require_http_methods(["GET"])
def recent_logs_api(request):
    # Fetch last 10 alerts (adjust as needed)
    alerts = Alert.objects.select_related('account').order_by('-timestamp')[:10]
    logs = []
    for alert in alerts:
        task_id = ''
        if alert.message.startswith('Started'):
            match = re.search(r'Started.*?(\btask_\w+\b)', alert.message)
            if match:
                task_id = match.group(1)
        logs.append({
            'timestamp': alert.timestamp.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3],  # Mimic Celery log format
            'severity': alert.severity.upper(),  # e.g., INFO, ERROR
            'message': alert.message,
            'account': alert.account.username if alert.account else 'System',
            'task_id': task_id
        })
    return JsonResponse({'logs': logs})
