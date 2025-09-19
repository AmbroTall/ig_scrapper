# from django.utils import timezone
# from django.views import View
# from django.shortcuts import render, redirect
# from django.contrib.auth.mixins import LoginRequiredMixin
# from .models import Account, Alert
# from .tasks import scrape_task, filter_task
# import csv
# from io import TextIOWrapper
# import random
# 
# class InputFormView(LoginRequiredMixin, View):
#     def get(self, request):
#         return render(request, 'input_form.html')
# 
#     def post(self, request):
#         hashtags = request.POST.get('hashtags', '').split(',')
#         locations = request.POST.get('locations', '').split(',')
#         followers = request.POST.get('followers', '').split(',')
#         following = request.POST.get('following', '').split(',')
#         likes = request.POST.get('likes', '').split(',')
#         comments = request.POST.get('comments', '').split(',')
#         tags = request.POST.get('tags', '').split(',')
#         professions = request.POST.get('professions', '').split(',')
#         countries = request.POST.get('countries', '').split(',')
#         keywords = request.POST.get('keywords', '').split(',')
# 
#         accounts = Account.objects.all()
#         for account in accounts:
#             # Reset daily scrape count if new day
#             if (timezone.now() - account.last_active).days >= 1:
#                 account.users_scraped = 0
#                 account.save()
#             for hashtag in hashtags:
#                 if hashtag.strip():
#                     delay = random.randint(300, 900)  # 5–15 min
#                     scrape_task.apply_async((account.id, "hashtag", hashtag.strip()), countdown=delay)
#             for location in locations:
#                 if location.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "location", location.strip()), countdown=delay)
#             for follower in followers:
#                 if follower.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "followers", follower.strip()), countdown=delay)
#             for following_user in following:
#                 if following_user.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "following", following_user.strip()), countdown=delay)
#             for like_url in likes:
#                 if like_url.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "likes", like_url.strip()), countdown=delay)
#             for comment_url in comments:
#                 if comment_url.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "comments", comment_url.strip()), countdown=delay)
#             for tag in tags:
#                 if tag.strip():
#                     delay = random.randint(300, 900)
#                     scrape_task.apply_async((account.id, "tags", tag.strip()), countdown=delay)
# 
#         filter_task.delay(professions, countries, keywords)
#         return redirect('status')
# 
# class StatusView(LoginRequiredMixin, View):
#     def get(self, request):
#         accounts = Account.objects.all()
#         alerts = Alert.objects.order_by('-timestamp')[:10]
#         return render(request, 'status.html', {'accounts': accounts, 'alerts': alerts})
# 
# class AccountUploadView(LoginRequiredMixin, View):
#     def get(self, request):
#         return render(request, 'accounts.html')
# 
#     def post(self, request):
#         csv_file = request.FILES.get('csv_file')
#         if csv_file:
#             csv_reader = csv.reader(TextIOWrapper(csv_file, 'utf-8'))
#             next(csv_reader)  # Skip header
#             for row in csv_reader:
#                 if len(row) >= 2:
#                     Account.objects.get_or_create(
#                         username=row[0],
#                         defaults={
#                             'password': row[1],
#                             'status': 'idle',
#                             'last_login': None,
#                             'relogin_attempt': 0,
#                             'warmed_up': False,
#                             'account_age_days': 0,  # Update manually
#                             'daily_scrape_limit': 20,
#                         }
#                     )
#         return redirect('status')