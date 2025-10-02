from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db.models import Q
from django.shortcuts import render, redirect
from django.contrib import messages
from django.views import View
from django.utils import timezone
from .models import Account, Alert, ScrapedUser, DMCampaign, DMTemplate
from .tasks import scrape_users_task, send_dms_task
import csv
from io import TextIOWrapper
import random

class InputFormView(LoginRequiredMixin, View):
    """View to handle input for scraping parameters"""
    def get(self, request):
        return render(request, 'dmbot/input_form.html')

    def post(self, request):
        try:
            # Extract and validate inputs
            hashtags = [h.strip() for h in request.POST.get('hashtags', '').split(',') if h.strip()]
            locations = [l.strip() for l in request.POST.get('locations', '').split(',') if l.strip()]
            tags = [t.strip() for t in request.POST.get('tags', '').split(',') if t.strip()]

            # Validate at least one source is provided
            if not any([hashtags, locations, tags]):
                messages.error(request, "At least one scraping source (e.g., hashtag, location) is required.")
                return render(request, 'dmbot/input_form.html')

            # Get healthy accounts
            accounts = Account.objects.filter(status='idle', health_score__gte=50)
            if not accounts:
                messages.error(request, "No healthy accounts available for scraping.")
                return render(request, 'dmbot/input_form.html')
            scrape_users_task.delay(accounts[0].id, "hashtag", hashtags[0])
            # Schedule scraping tasks with randomized delays
            for account in accounts[1:]:
                # Reset daily counters if needed
                account.reset_daily_counters()
                for hashtag in hashtags:
                    delay = random.randint(300, 900)  # 5–15 min
                    scrape_users_task.apply_async((account.id, "hashtag", hashtag), countdown=delay)
                for location in locations:
                    delay = random.randint(300, 900)
                    scrape_users_task.apply_async((account.id, "location", location), countdown=delay)
                for tag in tags:
                    delay = random.randint(300, 900)
                    scrape_users_task.apply_async((account.id, "tags", tag), countdown=delay)

            # # Schedule filtering task
            # if any([professions, countries, keywords]):
            #     filter_users_task.delay(professions, countries, keywords)

            messages.success(request, "Scraping tasks scheduled successfully.")
            return redirect('status')
        except Exception as e:
            messages.error(request, f"Error scheduling tasks: {str(e)}")
            return render(request, 'dmbot/input_form.html')


class StatusView(LoginRequiredMixin, View):
    def get(self, request):
        # Get search queries
        account_search = request.GET.get('account_search', '').strip()
        campaign_search = request.GET.get('campaign_search', '').strip()

        # Get all accounts with search filter
        accounts_list = Account.objects.select_related().all()
        if account_search:
            accounts_list = accounts_list.filter(
                Q(username__icontains=account_search) |
                Q(status__icontains=account_search)
            )

        accounts_paginator = Paginator(accounts_list, 5)  # 10 accounts per page
        accounts_page = request.GET.get('accounts_page', 1)

        try:
            accounts = accounts_paginator.page(accounts_page)
        except PageNotAnInteger:
            accounts = accounts_paginator.page(1)
        except EmptyPage:
            accounts = accounts_paginator.page(accounts_paginator.num_pages)

        # Get campaigns with search filter
        campaigns_list = DMCampaign.objects.filter(is_active=True)
        if campaign_search:
            campaigns_list = campaigns_list.filter(
                Q(name__icontains=campaign_search) |
                Q(template__name__icontains=campaign_search)
            )

        campaigns_paginator = Paginator(campaigns_list, 5)  # 10 campaigns per page
        campaigns_page = request.GET.get('campaigns_page', 1)

        try:
            campaigns = campaigns_paginator.page(campaigns_page)
        except PageNotAnInteger:
            campaigns = campaigns_paginator.page(1)
        except EmptyPage:
            campaigns = campaigns_paginator.page(campaigns_paginator.num_pages)

        # Non-paginated items
        alerts = Alert.objects.order_by('-timestamp')[:5]
        pending_enrichment = ScrapedUser.objects.filter(details_fetched=False).count()
        unclassified_users = ScrapedUser.objects.filter(
            Q(profession='') | Q(country=''), biography__isnull=False
        ).count()
        templates = DMTemplate.objects.filter(active=True)

        return render(request, 'dmbot/status.html', {
            'accounts': accounts,
            'alerts': alerts,
            'pending_enrichment': pending_enrichment,
            'unclassified_users': unclassified_users,
            'campaigns': campaigns,
            'templates': templates,
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
    """View to create and manage DM templates"""
    def get(self, request):
        templates = DMTemplate.objects.all()
        categories = ['general', 'photography', 'art', 'travel', 'business', 'other']  # Predefined categories
        return render(request, 'dmbot/template_form.html', {'templates': templates, 'categories': categories})

    def post(self, request):
        try:
            name = request.POST.get('name')
            template_text = request.POST.get('template')
            category = request.POST.get('category', 'general')  # Default to 'general' if empty
            if not name or not template_text:
                messages.error(request, "Name and template text are required.")
                return render(request, 'dmbot/template_form.html', {
                    'templates': DMTemplate.objects.all(),
                    'categories': ['general', 'photography', 'art', 'travel', 'business', 'other']
                })
            DMTemplate.objects.create(
                name=name,
                template=template_text,
                category=category or 'general',  # Ensure non-empty category
                active=True
            )
            messages.success(request, f"Template '{name}' created successfully.")
            return redirect('campaign')  # Redirect to /campaign/
        except Exception as e:
            messages.error(request, f"Error creating template: {str(e)}")
            return render(request, 'dmbot/template_form.html', {
                'templates': DMTemplate.objects.all(),
                'categories': ['general', 'photography', 'art', 'travel', 'business', 'other']
            })


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
        paginator = Paginator(users_list, 20)  # 20 users per page
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