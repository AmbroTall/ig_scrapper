
## 5. Advanced Filtering System (filter.py)

import logging
import re
from django.db.models import Q
from .models import ScrapedUser
import openai
from django.conf import settings


class UserFilter:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def filter_users(self, professions=None, countries=None, keywords=None,
                     min_followers=None, max_followers=None, activity_days=30):
        """Advanced user filtering with multiple criteria"""

        queryset = ScrapedUser.objects.filter(is_active=True)

        # Filter by professions
        if professions:
            profession_q = Q()
            for profession in professions:
                profession_q |= Q(profession__icontains=profession)
            queryset = queryset.filter(profession_q)

        # Filter by countries
        if countries:
            queryset = queryset.filter(country__in=countries)

        # Filter by keywords in bio
        if keywords:
            keyword_q = Q()
            for keyword in keywords:
                keyword_q |= Q(biography__icontains=keyword)
            queryset = queryset.filter(keyword_q)

        # Filter by follower count
        if min_followers:
            queryset = queryset.filter(follower_count__gte=min_followers)
        if max_followers:
            queryset = queryset.filter(follower_count__lte=max_followers)

        # Filter by activity
        if activity_days:
            from django.utils import timezone
            from datetime import timedelta
            cutoff_date = timezone.now() - timedelta(days=activity_days)
            queryset = queryset.filter(last_post_date__gte=cutoff_date)

        # Remove duplicates and already contacted users
        queryset = queryset.filter(dm_sent=False).distinct()

        return queryset

    def classify_users_ai(self, batch_size=50):
        """Use AI to classify users by profession and location"""

        unclassified_users = ScrapedUser.objects.filter(
            Q(profession='') | Q(country=''),
            biography__isnull=False
        ).exclude(biography='')[:batch_size]

        classified_count = 0

        for user in unclassified_users:
            try:
                classification = self._classify_single_user(user)
                if classification:
                    user.profession = classification.get('profession', '')
                    user.country = classification.get('country', '')
                    user.category_confidence = classification.get('confidence', 0.0)
                    user.save()
                    classified_count += 1

            except Exception as e:
                self.logger.error(f"Failed to classify user {user.username}: {e}")
                continue

        self.logger.info(f"Classified {classified_count} users")
        return classified_count

    def _classify_single_user(self, user):
        """Classify a single user using AI"""
        if not hasattr(settings, 'OPENAI_API_KEY') or not settings.OPENAI_API_KEY:
            return self._classify_with_rules(user)

        try:
            client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)

            prompt = f"""
            Analyze this Instagram user's biography and classify them:

            Username: {user.username}
            Bio: {user.biography}
            Followers: {user.follower_count}
            Following: {user.following_count}

            Please extract:
            1. Profession/Industry (e.g., photographer, artist, entrepreneur, student, etc.)
            2. Country/Location (if mentioned)
            3. Confidence level (0.0-1.0) for your classification

            Format your response as JSON:
            {{
                "profession": "profession_name",
                "country": "country_name",
                "confidence": 0.8
            }}

            If you can't determine something, use empty string. Be specific with professions.
            """

            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.3
            )

            import json
            result = json.loads(response.choices[0].message.content)
            return result

        except Exception as e:
            self.logger.error(f"AI classification failed for {user.username}: {e}")
            return self._classify_with_rules(user)

    def _classify_with_rules(self, user):
        """Fallback rule-based classification"""
        bio = user.biography.lower()

        # Profession patterns
        profession_patterns = {
            'photographer': ['photo', 'camera', 'shoot', 'portrait', 'wedding'],
            'artist': ['art', 'paint', 'draw', 'creative', 'design'],
            'entrepreneur': ['ceo', 'founder', 'startup', 'business', 'company'],
            'influencer': ['influencer', 'content creator', 'blogger', 'youtuber'],
            'fitness': ['fitness', 'gym', 'trainer', 'workout', 'health'],
            'food': ['chef', 'cook', 'food', 'restaurant', 'recipe'],
            'travel': ['travel', 'explore', 'adventure', 'nomad', 'wanderlust'],
            'fashion': ['fashion', 'style', 'model', 'clothing', 'outfit'],
            'music': ['music', 'musician', 'singer', 'dj', 'producer'],
            'tech': ['developer', 'engineer', 'tech', 'code', 'programming'],
        }

        detected_profession = ''
        max_matches = 0

        for profession, keywords in profession_patterns.items():
            matches = sum(1 for keyword in keywords if keyword in bio)
            if matches > max_matches:
                max_matches = matches
                detected_profession = profession

        # Country patterns (basic)
        country_patterns = {
            'usa': ['usa', 'america', 'us', 'new york', 'california', 'texas'],
            'uk': ['uk', 'london', 'england', 'britain'],
            'canada': ['canada', 'toronto', 'vancouver'],
            'australia': ['australia', 'sydney', 'melbourne'],
            'germany': ['germany', 'berlin', 'munich'],
            'france': ['france', 'paris', 'lyon'],
            'italy': ['italy', 'rome', 'milan'],
            'spain': ['spain', 'madrid', 'barcelona'],
        }

        detected_country = ''
        for country, keywords in country_patterns.items():
            if any(keyword in bio for keyword in keywords):
                detected_country = country
                break

        confidence = 0.6 if detected_profession else 0.3

        return {
            'profession': detected_profession,
            'country': detected_country,
            'confidence': confidence
        }