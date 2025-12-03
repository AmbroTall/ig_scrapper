import logging
import os
import re
from datetime import datetime

import pandas as pd
import redis
from celery import Celery, current_app
from celery.app.control import Inspect
from celery.result import AsyncResult
from django.conf import settings
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db import transaction
from django.db.models import Q
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.utils.dateparse import parse_datetime
from django.views import View
from django.utils import timezone
from django.views.decorators.http import require_http_methods
from django.conf import settings  # Import settings for TIME_ZONE
from zoneinfo import ZoneInfo

from django.views.generic import ListView

from .forms import DMCsvUploadForm
from .models import Account, Alert, ScrapedUser, DMCampaign, DMTemplate, DMLog, DMCsvUpload, ScheduledTask, \
    FallbackMessage, DailyMetric
from .tasks import scrape_users_task, send_dms_task, cancel_task, redis_client, enrich_user_details_task
import csv
from io import TextIOWrapper
import random

from .utils import send_alert

# Initialize Celery app (adjust this based on your Celery configuration)
app = Celery('app', broker='redis://localhost:6379/0')  # Update with your broker URL


class InputFormView(View):
    """View to handle input for scraping parameters"""

    def get(self, request):
        accounts = Account.objects.filter(status='idle', health_score__gte=50)
        return render(request, 'dmbot/input_form.html', {'accounts': accounts})

    def post(self, request):
        try:
            # ✅ CHECK DAILY LIMIT BEFORE SCHEDULING
            today_metric, _ = DailyMetric.objects.get_or_create(date=timezone.now().date())
            if today_metric.scraped_count >= today_metric.scraping_threshold:
                messages.error(
                    request,
                    f"Daily scraping limit reached ({today_metric.scraped_count}/{today_metric.scraping_threshold}). "
                    "Please try again tomorrow."
                )
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })
            hashtags = [h.strip() for h in request.POST.get('hashtags', '').split(',') if h.strip()]
            locations = [l.strip() for l in request.POST.get('locations', '').split(',') if l.strip()]
            tags = [t.strip() for t in request.POST.get('tags', '').split(',') if t.strip()]
            account_ids = request.POST.getlist('accounts')
            schedule_time = request.POST.get('schedule_time', '')

            if not any([hashtags, locations, tags]):
                messages.error(request, "At least one scraping source is required.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            if not account_ids:
                messages.error(request, "At least one account must be selected.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            accounts = Account.objects.filter(id__in=account_ids, status='idle', health_score__gte=50)
            if not accounts:
                messages.error(request, "No valid accounts selected.")
                return render(request, 'dmbot/input_form.html', {
                    'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                })

            eta = None
            if schedule_time:
                eta = parse_datetime(schedule_time)
                eta = eta.replace(tzinfo=timezone.get_current_timezone())
                if eta and eta < timezone.now():
                    messages.error(request, "Scheduled time must be in the future.")
                    return render(request, 'dmbot/input_form.html', {
                        'accounts': Account.objects.filter(status='idle', health_score__gte=50)
                    })

            first_account = True
            for account in accounts:
                account.reset_daily_counters()
                for hashtag in hashtags:
                    parameters = {'account_id': account.id, 'source_type': 'hashtag', 'source_value': hashtag}
                    if first_account and not eta:
                        task = scrape_users_task.delay(account.id, 'hashtag', hashtag)
                        ScheduledTask.objects.create(
                            task_type='scrape',
                            task_id=task.id,
                            scheduled_time=timezone.now(),
                            parameters=parameters
                        )
                        first_account = False
                    else:
                        delay = random.randint(50, 150) if not eta else 0
                        task = scrape_users_task.apply_async(
                            args=(account.id, 'hashtag', hashtag),
                            countdown=delay,
                            eta=eta
                        )
                        ScheduledTask.objects.create(
                            task_type='scrape',
                            task_id=task.id,
                            scheduled_time=eta or timezone.now() + timezone.timedelta(seconds=delay),
                            parameters=parameters
                        )
                for location in locations:
                    delay = random.randint(50, 150)
                    parameters = {'account_id': account.id, 'source_type': 'location', 'source_value': location}
                    task = scrape_users_task.apply_async(
                        args=(account.id, 'location', location),
                        countdown=delay,
                        eta=eta
                    )
                    ScheduledTask.objects.create(
                        task_type='scrape',
                        task_id=task.id,
                        scheduled_time=eta or timezone.now() + timezone.timedelta(seconds=delay),
                        parameters=parameters
                    )
                for tag in tags:
                    delay = random.randint(50, 150)
                    parameters = {'account_id': account.id, 'source_type': 'tags', 'source_value': tag}
                    task = scrape_users_task.apply_async(
                        args=(account.id, 'tags', tag),
                        countdown=delay,
                        eta=eta
                    )
                    ScheduledTask.objects.create(
                        task_type='scrape',
                        task_id=task.id,
                        scheduled_time=eta or timezone.now() + timezone.timedelta(seconds=delay),
                        parameters=parameters
                    )

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
        processed_activities = []
        inspector = current_app.control.inspect()

        # Fetch active tasks
        try:
            active_tasks = inspector.active() or {}
            for worker, tasks in active_tasks.items():
                for task in tasks:
                    task_id = task.get('id')
                    task_name = task.get('name', 'Unknown Task')
                    username = 'System'
                    try:
                        accounts = Account.objects.filter(task_id=task_id)
                        if accounts.exists():
                            username = ', '.join(account.username for account in accounts)
                    except Exception as e:
                        logging.error(f"Error fetching accounts for task {task_id}: {e}")
                    if search_query:
                        if not (search_query.lower() in task_name.lower() or
                                search_query.lower() in username.lower()):
                            continue
                    processed_activities.append({
                        'task_id': task_id,
                        'message': f"Running task: {task_name}",
                        'username': username,
                        'timestamp': task.get('time_start', None),
                        'status': 'running'
                    })
        except Exception as e:
            logging.error(f"Error fetching active tasks: {e}")
            send_alert(f"Error fetching active tasks: {e}", "error")

        # Fetch scheduled tasks
        scheduled_tasks = ScheduledTask.objects.filter(status='scheduled')
        if search_query:
            scheduled_tasks = scheduled_tasks.filter(
                Q(task_type__icontains=search_query) |
                Q(parameters__icontains=search_query)
            )
        for task in scheduled_tasks:
            username = 'System'
            if task.task_type == 'scrape':
                account = Account.objects.filter(id=task.parameters.get('account_id')).first()
                username = account.username if account else 'System'
            elif task.task_type in ['dm_campaign', 'dm_csv']:
                account_ids = task.parameters.get('account_ids', [])
                accounts = Account.objects.filter(id__in=account_ids)
                username = ', '.join(account.username for account in accounts) if accounts else 'System'
            elif task.task_type == 'enrich':
                account_ids = task.parameters.get('account_ids', [])
                accounts = Account.objects.filter(id__in=account_ids)
                username = ', '.join(account.username for account in accounts) if accounts else 'System'

            processed_activities.append({
                'task_id': task.task_id,
                'message': f"Scheduled {task.get_task_type_display()}: {task.parameters.get('source_value', task.parameters.get('name', ''))}",
                'username': username,
                'timestamp': task.scheduled_time.timestamp(),
                'status': 'scheduled'
            })

        # Sort activities by timestamp (newest first)
        processed_activities.sort(key=lambda x: x['timestamp'] or 0, reverse=True)

        paginator = Paginator(processed_activities, 10)
        page_number = request.GET.get('page', 1)
        activities_paginated = paginator.get_page(page_number)

        return render(request, 'dmbot/bot_activity.html', {
            'activities': activities_paginated,
            'search_query': search_query
        })

    def post(self, request):
        # if request.headers.get('X-Requested-With') != 'XMLHttpRequest':
        #     logging.warning("Non-AJAX request to BotActivityView.post")
        #     return JsonResponse({'status': 'error', 'message': 'Invalid request'}, status=400)

        try:
            # Handle bulk task cancellation
            if request.POST.get('cancel_all') == 'true':
                inspector = current_app.control.inspect()
                active_tasks = inspector.active() or {}
                cancelled_tasks = []
                for worker, tasks in active_tasks.items():
                    for task in tasks:
                        task_id = task.get('id')
                        task_name = task.get('name', 'Unknown Task')
                        try:
                            # Revoke task
                            AsyncResult(task_id).revoke(terminate=True)
                            logging.info(f"Task {task_id} ({task_name}) cancelled")
                            send_alert(f"Task {task_id} ({task_name}) cancelled", "info")
                            cancelled_tasks.append(task_id)

                            # Reset accounts associated with this task_id
                            accounts = Account.objects.filter(task_id=task_id)
                            for account in accounts:
                                account.status = 'idle'
                                account.task_id = None
                                account.save()
                                logging.info(
                                    f"Reset account {account.username} to idle after cancelling task {task_id}")
                                send_alert(f"Reset account {account.username} to idle after cancelling task {task_id}",
                                           "info", account)

                            # Release locks for specific tasks
                            if task_name == 'dmbot.tasks.scrape_users_task':
                                redis_client.delete("scraping_lock")
                            elif task_name == 'dmbot.tasks.enrich_user_details_task':
                                redis_client.delete("enrichment_lock")
                        except Exception as e:
                            logging.error(f"Failed to cancel task {task_id}: {e}")
                            send_alert(f"Failed to cancel task {task_id}: {e}", "error")
                return JsonResponse({'status': 'cancelled', 'message': f'Cancelled {len(cancelled_tasks)} tasks'})

            # Handle single task cancellation
            task_id = request.POST.get('task_id')
            if task_id:
                try:
                    task = AsyncResult(task_id)
                    task_name = task.name or 'Unknown Task'
                    # Revoke task
                    task.revoke(terminate=True)
                    logging.info(f"Task {task_id} ({task_name}) cancelled")
                    send_alert(f"Task {task_id} ({task_name}) cancelled", "info")

                    # Reset accounts associated with this task_id
                    accounts = Account.objects.filter(task_id=task_id)
                    for account in accounts:
                        account.status = 'idle'
                        account.task_id = None
                        account.save()
                        logging.info(f"Reset account {account.username} to idle after cancelling task {task_id}")
                        send_alert(f"Reset account {account.username} to idle after cancelling task {task_id}", "info",
                                   account)

                    # Release locks for specific tasks
                    if task_name == 'dmbot.tasks.scrape_users_task':
                        redis_client.delete("scraping_lock")
                    elif task_name == 'dmbot.tasks.enrich_user_details_task':
                        redis_client.delete("enrichment_lock")
                    messages.success(request,f"Task {task_id} cancelled.")
                    return redirect('status')
                except Exception as e:
                    logging.error(f"Failed to cancel task {task_id}: {e}")
                    send_alert(f"Failed to cancel task {task_id}: {e}", "error")
                    messages.error(request, f"Failed to cancel task {task_id}: {str(e)}.")
                    return redirect('status')
            messages.error(request, f"No task_id or cancel_all provided.")
            return redirect('status')
        except Exception as e:
            logging.error(f"Error in BotActivityView.post: {e}")
            send_alert(f"Error in BotActivityView.post: {e}", "error")
            messages.error(request, f"Error processing request: {str(e)}")
            return redirect('status')


class StatusView(View):
    def get(self, request):
        with transaction.atomic():
            accounts = Account.objects.all().order_by('-last_active')
            accounts_count = accounts.count()
            campaigns = DMCampaign.objects.select_related('template').all().order_by('-created_at')
            campaigns_count = campaigns.count()
            templates = DMTemplate.objects.filter(active=True)
            alerts = Alert.objects.filter(severity__in=['error', 'warning', 'critical']).order_by('-timestamp')[:5]
            pending_enrichment = ScrapedUser.objects.filter(details_fetched=False).count()
            enriched_users = ScrapedUser.objects.filter(details_fetched=True).count()
            total_dms_sent = DMLog.objects.count()
            csv_campaigns = DMCsvUpload.objects.filter(processed=False)
            today = timezone.now().date()
            metrics = DailyMetric.objects.filter(date=today).first() or DailyMetric(date=today)
            scheduled_tasks = ScheduledTask.objects.filter(
                status__in=['scheduled', 'running']
            ).order_by('scheduled_time')

            account_search = request.GET.get('account_search', '')
            campaign_search = request.GET.get('campaign_search', '')

            if account_search:
                accounts = accounts.filter(
                    Q(username__icontains=account_search) |
                    Q(status__icontains=account_search)
                )
                accounts_count = accounts.count()
            if campaign_search:
                campaigns = campaigns.filter(
                    Q(name__icontains=campaign_search) |
                    Q(template__name__icontains=campaign_search)
                )
                campaigns_count = campaigns.count()

            accounts_paginator = Paginator(accounts, 10) if accounts_count > 5 else None
            campaigns_paginator = Paginator(campaigns, 5) if campaigns_count > 5 else None
            scheduled_tasks_paginator = Paginator(scheduled_tasks, 5) if scheduled_tasks.count() > 5 else None

            accounts_page = request.GET.get('accounts_page', 1)
            campaigns_page = request.GET.get('campaigns_page', 1)
            scheduled_tasks_page = request.GET.get('scheduled_tasks_page', 1)

            accounts_paginated = accounts_paginator.get_page(accounts_page) if accounts_paginator else accounts
            campaigns_paginated = campaigns_paginator.get_page(campaigns_page) if campaigns_paginator else campaigns
            scheduled_tasks_paginated = scheduled_tasks_paginator.get_page(
                scheduled_tasks_page) if scheduled_tasks_paginator else scheduled_tasks
            # In your view
            scraping_remaining = max(0, metrics.scraping_threshold - metrics.scraped_count)

        return render(request, 'dmbot/status.html', {
            'accounts': accounts_paginated,
            'accounts_count': accounts_count,
            'campaigns': campaigns_paginated,
            'campaigns_count': campaigns_count,
            'templates': templates,
            'alerts': alerts,
            'pending_enrichment': pending_enrichment,
            'enriched_users': enriched_users,
            'account_search': account_search,
            'campaign_search': campaign_search,
            'total_dms_sent': total_dms_sent,
            'csv_campaigns': csv_campaigns,
            'scheduled_tasks': scheduled_tasks_paginated,
            'scheduled_tasks_count': scheduled_tasks.count(),
            'metrics': metrics,
            'scraping_remaining': scraping_remaining
        })

    def post(self, request):
        # if not request.user.is_authenticated:
        #     return redirect('%s?next=%s' % (settings.LOGIN_URL, request.path))
        today = timezone.now().date()
        if "set_scraping_limit" in request.POST:
            try:
                new_limit = int(request.POST.get("scraping_threshold", 45000))
                metric, created = DailyMetric.objects.get_or_create(date=today)
                old_limit = metric.scraping_threshold
                metric.scraping_threshold = new_limit
                if metric.scraped_count < new_limit:
                    metric.scraping_limit_reached = False
                metric.save()
                messages.success(
                    request,
                    f"Daily scraping limit updated: {old_limit:,} → {new_limit:,} users"
                )
            except (ValueError, TypeError):
                messages.error(request, "Invalid number entered.")
        return redirect('status')



class AccountUploadView(View):
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
                            'warmed_up': True,
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


class AlertAcknowledgeView(View):
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


class DMCsvUploadView(View):
    def get(self, request):
        form = DMCsvUploadForm()
        return render(request, 'dmbot/csv_upload.html', {'form': form})

    def post(self, request):
        form = DMCsvUploadForm(request.POST, request.FILES)
        schedule_time = request.POST.get('schedule_time', '')
        eta = None
        if schedule_time:
            eta = parse_datetime(schedule_time)
            eta = eta.replace(tzinfo=timezone.get_current_timezone())
            if eta and eta < timezone.now():
                messages.error(request, "Scheduled time must be in the future.")
                return render(request, 'dmbot/csv_upload.html', {'form': form})

        if not form.is_valid():
            messages.error(request, "Invalid form submission.")
            return render(request, 'dmbot/csv_upload.html', {'form': form})

        try:
            # === 1. Create CSV Upload ===
            csv_upload = DMCsvUpload.objects.create(
                name=form.cleaned_data['name'],
                csv_file=form.cleaned_data['csv_file'],
            )
            account_ids = form.cleaned_data['accounts']
            csv_upload.accounts.set(account_ids)

            # === 2. Validate CSV Structure ===
            csv_path = os.path.join(settings.MEDIA_ROOT, csv_upload.csv_file.name)
            df = pd.read_csv(csv_path)
            required_columns = ['username', 'message']
            if not all(col in df.columns for col in required_columns):
                messages.error(request, "CSV must contain 'username' and 'message' columns.")
                csv_upload.delete()
                return render(request, 'dmbot/csv_upload.html', {'form': form})

            # === 3. Validate Accounts (in View) ===
            valid_accounts = Account.objects.filter(
                id__in=account_ids,
                status='idle',
                warmed_up=True
            )
            if not valid_accounts.exists():
                messages.error(request, "No valid (idle & warmed up) accounts selected.")
                csv_upload.delete()
                return render(request, 'dmbot/csv_upload.html', {'form': form})

            # === 4. Schedule ONE Task PER Account ===
            tasks_created = 0
            for account in valid_accounts:
                task = send_dms_task.apply_async(
                    kwargs={
                        'csv_upload_id': csv_upload.id,
                        'account_id': account.id,
                        'mode': 'csv',
                        'max_dms_per_account': 15  # or make configurable
                    },
                    eta=eta
                )
                ScheduledTask.objects.create(
                    task_type='dm_csv',
                    task_id=task.id,
                    scheduled_time=eta or timezone.now(),
                    parameters={
                        'csv_upload_id': csv_upload.id,
                        'account_id': account.id,
                        'name': csv_upload.name
                    }
                )
                tasks_created += 1

            messages.success(
                request,
                f"CSV campaign '{csv_upload.name}' scheduled: {tasks_created} account task(s) created."
            )
            return redirect('status')

        except Exception as e:
            messages.error(request, f"Error processing CSV: {str(e)}")
            if 'csv_upload' in locals():
                try:
                    csv_upload.delete()
                except:
                    pass
            return render(request, 'dmbot/csv_upload.html', {'form': form})


class DMCampaignView(View):
    def get(self, request):
        templates = DMTemplate.objects.filter(active=True)
        if not templates:
            messages.error(request, "No active DM templates available. Please create a template first.")
            return redirect('template_form')
        accounts = Account.objects.filter(status='idle', warmed_up=True)
        # Pagination for fallback messages in modal
        fallback_qs = FallbackMessage.objects.all()
        paginator = Paginator(fallback_qs, 5)  # 5 per page
        page = request.GET.get('fallback_page')
        fallback_messages = paginator.get_page(page)

        return render(request, 'dmbot/campaign_form.html', {
            'templates': templates,
            'accounts': accounts,
            'mode': 'campaign',  # Default mode
            'active_fallback': FallbackMessage.objects.filter(is_active=True).first(),
            'fallback_messages': fallback_messages,
        })

    def post(self, request):
        mode = request.POST.get('mode', 'campaign')
        schedule_time = request.POST.get('schedule_time', '')
        eta = None
        if schedule_time:
            eta = parse_datetime(schedule_time)
            eta = eta.replace(tzinfo=timezone.get_current_timezone())
            if eta and eta < timezone.now():
                messages.error(request, "Scheduled time must be in the future.")
                return self._render_form(mode)

        if mode == 'campaign':
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
                    return self._render_form(mode)

                # Create campaign
                campaign = DMCampaign.objects.create(
                    name=name,
                    template_id=template_id,
                    target_filters=filters
                )
                campaign.accounts.set(account_ids)

                # Validate accounts in view
                valid_accounts = Account.objects.filter(
                    id__in=account_ids,
                    status='idle',
                    warmed_up=True
                )
                if not valid_accounts.exists():
                    messages.error(request, "No valid accounts selected.")
                    campaign.delete()
                    return self._render_form(mode)

                # Schedule ONE task PER account
                tasks_created = 0

                for account in valid_accounts:
                    task = send_dms_task.apply_async(
                        kwargs={
                            'campaign_id': campaign.id,
                            'account_id': account.id,  # ← NEW
                            'mode': 'campaign',
                            'max_dms_per_account': 15
                        },
                        eta=eta
                    )
                    ScheduledTask.objects.create(
                        task_type='dm_campaign',
                        task_id=task.id,
                        scheduled_time=eta or timezone.now(),
                        parameters={
                            'campaign_id': campaign.id,
                            'account_id': account.id,
                            'name': name
                        }
                    )
                    tasks_created += 1

                messages.success(request, f"Campaign '{name}' scheduled: {tasks_created} account tasks.")
                return redirect('status')

            except Exception as e:
                messages.error(request, f"Error: {str(e)}")
                return self._render_form(mode)
        else:
            return redirect('csv_upload')

    def _render_form(self, mode='campaign'):
        return render(self.request, 'dmbot/campaign_form.html', {
            'templates': DMTemplate.objects.filter(active=True),
            'accounts': Account.objects.filter(status='idle', warmed_up=True),
            'mode': mode
        })


class DMTemplateView(View):
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


class ScrapedUsersView(View):
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
    template_name = 'dmbot/account_management.html'

    def get(self, request):
        search_query = request.GET.get('search', '').strip()
        accounts = Account.objects.all().order_by('-health_score', 'username')

        if search_query:
            accounts = accounts.filter(
                Q(username__icontains=search_query) |
                Q(status__icontains=search_query)
            )

        paginator = Paginator(accounts, 15)
        page_number = request.GET.get('page', 1)
        page_obj = paginator.get_page(page_number)
        total_accounts = Account.objects.count()
        healthy_accounts = Account.objects.filter(health_score__gte=70).count()
        flagged_accounts = Account.objects.filter(health_score__lt=50).count()

        return render(request, self.template_name, {
            'accounts': page_obj,
            'search_query': search_query,
            'total_accounts': Account.objects.count(),
            'healthy_accounts': Account.objects.filter(health_score__gte=70).count(),
            'flagged_accounts': Account.objects.filter(health_score__lt=50).count(),
            'needs_attention': total_accounts - healthy_accounts - flagged_accounts,

        })

    def post(self, request):
        action = request.POST.get('action')
        account_id = request.POST.get('account_id')

        if not account_id:
            messages.error(request, "No account selected.")
            return redirect('account_management')

        account = get_object_or_404(Account, id=account_id)

        if action == 'delete':
            username = account.username
            account.delete()
            messages.success(request, f"Account @{username} deleted permanently.")

        elif action == 'reset_counters':
            account.users_scraped_today = 0
            account.dms_sent_today = 0
            account.last_reset = timezone.now().date()
            account.save()
            messages.success(request, f"Daily counters reset for @{account.username}")

        elif action == 'mark_warmed':
            account.warmed_up = True
            account.warmup_level = 5
            account.health_score = min(100, account.health_score + 20)
            account.save()
            messages.success(request, f"@{account.username} marked as fully warmed up")

        elif action == 'force_idle':
            account.status = 'idle'
            account.task_id = None
            account.save()
            messages.success(request, f"@{account.username} forced to idle state")

        elif action == 'update_limits':
            try:
                account.daily_scrape_limit = int(request.POST.get('daily_scrape_limit', 50))
                account.daily_dm_limit = int(request.POST.get('daily_dm_limit', 15))
                account.save()
                messages.success(request, f"Limits updated for @{account.username}")
            except ValueError:
                messages.error(request, "Invalid limit values")

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


# class EnrichmentFormView(View):
#     """View to handle starting enrichment task with account selection"""
#
#     def get(self, request):
#         accounts = Account.objects.filter(status='idle', health_score__gte=50, warmed_up=True)
#         return render(request, 'dmbot/enrichment_form.html', {'accounts': accounts})
#
#     def post(self, request):
#         try:
#             account_ids = request.POST.getlist('accounts')
#             schedule_time = request.POST.get('schedule_time', '')
#             eta = None
#             if schedule_time:
#                 eta = parse_datetime(schedule_time)
#                 eta = eta.replace(tzinfo=timezone.get_current_timezone())
#
#                 if eta and eta < timezone.now():
#                     messages.error(request, "Scheduled time must be in the future.")
#                     return render(request, 'dmbot/enrichment_form.html', {
#                         'accounts': Account.objects.filter(status='idle', health_score__gte=50, warmed_up=True)
#                     })
#
#             if not account_ids:
#                 messages.error(request, "At least one account must be selected for enrichment.")
#                 return render(request, 'dmbot/enrichment_form.html', {
#                     'accounts': Account.objects.filter(status='idle', health_score__gte=50, warmed_up=True)
#                 })
#
#             accounts = Account.objects.filter(id__in=account_ids, status='idle', health_score__gte=50, warmed_up=True)
#             if not accounts:
#                 messages.error(request, "No valid accounts selected for enrichment.")
#                 return render(request, 'dmbot/enrichment_form.html', {
#                     'accounts': Account.objects.filter(status='idle', health_score__gte=50, warmed_up=True)
#                 })
#
#             selected_accounts_key = "enrichment_selected_accounts"
#             redis_client.delete(selected_accounts_key)
#             for account in accounts:
#                 redis_client.rpush(selected_accounts_key, account.id)
#             redis_client.expire(selected_accounts_key, 3600)
#
#             task = enrich_user_details_task.apply_async(eta=eta)
#             ScheduledTask.objects.create(
#                 task_type='enrich',
#                 task_id=task.id,
#                 scheduled_time=eta or timezone.now(),
#                 parameters={'account_ids': account_ids}
#             )
#             messages.success(request, "Enrichment task scheduled successfully.")
#             return redirect('status')
#         except Exception as e:
#             messages.error(request, f"Error scheduling enrichment: {str(e)}")
#             return render(request, 'dmbot/enrichment_form.html', {
#                 'accounts': Account.objects.filter(status='idle', health_score__gte=50, warmed_up=True)
#             })


class CancelScheduledTaskView(View):
    def post(self, request, task_id):
        try:
            scheduled_task = ScheduledTask.objects.get(id=task_id, status='scheduled')
            AsyncResult(scheduled_task.task_id).revoke(terminate=True)
            scheduled_task.status = 'cancelled'
            scheduled_task.save()
            messages.success(request, "Scheduled task cancelled successfully.")
        except ScheduledTask.DoesNotExist:
            messages.error(request, "Scheduled task not found or already started.")
        except Exception as e:
            messages.error(request, f"Error cancelling task: {str(e)}")
        return redirect('status')


# Full CRUD handler
def manage_fallbacks(request):
    if request.method == "POST":
        action = request.POST.get('action')
        fb_id = request.POST.get('fallback_id')

        if action == "create":
            fb = FallbackMessage.objects.create(
                name=request.POST['name'],
                message=request.POST['message'],
                is_active=True
            )
            messages.success(request, f"Created & activated: {fb.name}")

        elif action == "update":
            fb = FallbackMessage.objects.get(id=fb_id)
            fb.name = request.POST['name']
            fb.message = request.POST['message']
            fb.save()
            messages.success(request, f"Updated: {fb.name}")

        elif action == "activate":
            fb = FallbackMessage.objects.get(id=fb_id)
            fb.is_active = True
            fb.save()
            messages.success(request, f"Now active: {fb.name}")

        elif action == "delete":
            fb = FallbackMessage.objects.get(id=fb_id)
            name = fb.name
            fb.delete()
            messages.success(request, f"Deleted: {name}")

        return redirect('campaign')
    return redirect('campaign')


class DynamicExportView(View):
    def get(self, request, model_type):
        if model_type == "users":
            from .models import ScrapedUser
            queryset = ScrapedUser.objects.all().select_related('account')
            field_labels = {
                'username': 'Username', 'biography': 'Bio', 'profession': 'Profession',
                'country': 'Country', 'follower_count': 'Followers', 'following_count': 'Following',
                'post_count': 'Posts', 'last_post_date': 'Last Post', 'scraped_at': 'Scraped At',
                'is_active': 'Active', 'dm_sent': 'DM Sent', 'engagement_rate': 'Engagement %'
            }
            all_fields = list(field_labels.keys())
            title = "Export Scraped Users"
            template = "dmbot/export_users_modal.html"

        elif model_type == "dms":
            from .models import DMLog
            queryset = DMLog.objects.all().select_related('sender_account', 'recipient_user', 'campaign')
            field_labels = {
                'sender_account__username': 'From Account',
                'recipient_user__username': 'To User',
                'message': 'Message',
                'sent_at': 'Sent At',
                'campaign__name': 'Campaign',
            }
            all_fields = list(field_labels.keys())
            title = "Export Sent DMs"
            template = "dmbot/export_dms_modal.html"
        else:
            return HttpResponse("Invalid", status=400)

        # === APPLY FILTERS ===
        if request.GET:
            # Date range
            date_from = request.GET.get('date_from')
            date_to = request.GET.get('date_to')
            if model_type == "users":
                if date_from:
                    queryset = queryset.filter(scraped_at__date__gte=date_from)
                if date_to:
                    queryset = queryset.filter(scraped_at__date__lte=date_to)
                # Extra filters
                if request.GET.get('profession'):
                    queryset = queryset.filter(profession__icontains=request.GET['profession'])
                if request.GET.get('country'):
                    queryset = queryset.filter(country__icontains=request.GET['country'])
                if request.GET.get('dm_sent') == 'yes':
                    queryset = queryset.filter(dm_sent=True)
                if request.GET.get('dm_sent') == 'no':
                    queryset = queryset.filter(dm_sent=False)
                if request.GET.get('active') == 'yes':
                    queryset = queryset.filter(is_active=True)
                if request.GET.get('active') == 'no':
                    queryset = queryset.filter(is_active=False)

            elif model_type == "dms":
                if date_from:
                    queryset = queryset.filter(sent_at__date__gte=date_from)
                if date_to:
                    queryset = queryset.filter(sent_at__date__lte=date_to)

        # === EXPORT CSV ===
        selected_fields = request.GET.getlist('fields')
        if selected_fields and request.GET.get('download') == 'true':
            response = HttpResponse(content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{model_type}_export_{timezone.now().strftime("%Y%m%d_%H%M%S")}.csv"'
            writer = csv.writer(response)

            # Header
            headers = []
            for field in selected_fields:
                if model_type == "dms" and '__' in field:
                    headers.append(field_labels.get(field, field.split('__')[-1].title()))
                else:
                    headers.append(field_labels.get(field, field.replace('_', ' ').title()))
            writer.writerow(headers)

            # Data
            for obj in queryset.iterator():
                row = []
                for field in selected_fields:
                    if model_type == "dms" and '__' in field:
                        val = obj
                        for part in field.split('__'):
                            val = getattr(val, part, '') if val else ''
                        if hasattr(val, 'username'):
                            val = val.username
                        elif hasattr(val, 'name'):
                            val = val.name
                        elif isinstance(val, datetime):
                            val = val.strftime("%Y-%m-%d %H:%M")
                    else:
                        val = getattr(obj, field, '')
                        if isinstance(val, datetime):
                            val = val.strftime("%Y-%m-%d %H:%M")
                        elif isinstance(val, bool):
                            val = "Yes" if val else "No"
                    row.append(str(val) if val is not None else "")
                writer.writerow(row)
            return response

        # === SHOW MODAL ===
        context = {
            'title': title,
            'fields': [(f, field_labels.get(f, f.replace('_', ' ').title())) for f in all_fields],
            'total_count': queryset.count(),
            'model_type': model_type,
        }
        return render(request, template, context)


class DailyMetricsHistoryView(ListView):
    model = DailyMetric
    template_name = 'dmbot/daily_metrics_history.html'
    context_object_name = 'metrics'
    paginate_by = 30
    ordering = ['-date']

    def get_queryset(self):
        return DailyMetric.objects.all().order_by('-date')

    # In your DailyMetricsHistoryView.get_context_data()
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['today_metric'] = DailyMetric.objects.filter(date=timezone.now().date()).first()
        context['total_days'] = DailyMetric.objects.count()

        # ← ADD THESE 3 LINES
        context['total_scraped'] = sum(m.scraped_count for m in context['metrics'])
        context['total_enriched'] = sum(m.enriched_count for m in context['metrics'])
        context['total_dms'] = sum(m.dm_sent_count for m in context['metrics'])

        return context