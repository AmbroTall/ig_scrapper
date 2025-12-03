# forms.py
from django import forms

from .models import Account


class DMCsvUploadForm(forms.Form):
    name = forms.CharField(
        max_length=255,
        label="Campaign Name",
        widget=forms.TextInput(attrs={
            'class': 'w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent transition-shadow',
            'placeholder': 'e.g., Summer Sale Campaign 2025'
        })
    )
    csv_file = forms.FileField(label="CSV File")
    accounts = forms.ModelMultipleChoiceField(
        queryset=Account.objects.filter(status='idle', warmed_up=True),
        widget=forms.SelectMultiple(attrs={
            'class': 'w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary focus:border-transparent transition-shadow'
        }),
        label="Instagram Accounts"
    )