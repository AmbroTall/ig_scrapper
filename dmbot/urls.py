# from django.contrib import admin
# from django.urls import path
# from django.contrib.auth.views import LoginView, LogoutView
# from .views import InputFormView, StatusView, AccountUploadView
#
# urlpatterns = [
#     path('admin/', admin.site.urls),
#     path('login/', LoginView.as_view(template_name='login.html'), name='login'),
#     path('logout/', LogoutView.as_view(next_page='login'), name='logout'),
#     path('', InputFormView.as_view(), name='input_form'),
#     path('status/', StatusView.as_view(), name='status'),
#     path('accounts/', AccountUploadView.as_view(), name='accounts'),
# ]