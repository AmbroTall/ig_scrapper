## 2. Enhanced Scraper (scraper.py)

import logging
import time
import random
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, UserNotFound, PrivateError
from .models import ScrapedUser
from .utils import setup_client, send_alert

# Rate limiting decorator
def rate_limit(max_calls_per_hour=60):
    def decorator(func):
        def wrapper(self, account, *args, **kwargs):
            now = timezone.now()

            # Correctly check account.hour_reset
            if (now - account.hour_reset).total_seconds() > 3600:
                account.actions_this_hour = 0
                account.hour_reset = now
                account.save()

            if account.actions_this_hour >= account.daily_scrape_limit:
                raise Exception("Rate limit reached for this account")

            result = func(self, account, *args, **kwargs)

            account.actions_this_hour += 1
            account.last_action_time = now
            account.save()

            return result

        return wrapper

    return decorator


class InstagramScraper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def warm_up_account(self, account):
        """Enhanced warm-up with multiple levels"""
        cl = setup_client(account)
        print("Ambro", cl)
        if not cl:
            return False

        try:
            account.status = "warming_up"
            account.save()

            # Level-based warm-up activities
            warmup_activities = [
                lambda: self._browse_explore(cl),
                lambda: self._like_posts(cl, count=5),
                lambda: self._view_stories(cl, count=3),
                lambda: self._search_users(cl, count=2),
                lambda: self._browse_hashtags(cl, count=3),
            ]

            # Execute activities based on current warmup level
            max_level = min(account.warmup_level + 1, len(warmup_activities))

            for i in range(max_level):
                try:
                    warmup_activities[i]()
                    time.sleep(random.randint(30, 90))
                except Exception as e:
                    self.logger.warning(f"Warmup activity {i} failed for {account.username}: {e}")

            account.warmup_level = max_level
            account.warmed_up = max_level >= 3  # Consider warmed up after level 3
            account.last_active = timezone.now()
            account.update_health_score(True, 'warmup')

            self.logger.info(f"Warmed up {account.username} to level {max_level}")
            return True

        except ClientError as e:
            print(e)
            self.logger.error(f"Warm-up failed for {account.username}: {e}")
            account.update_health_score(False, 'warmup')
            return False
        finally:
            account.status = "idle"
            account.save()

    def _browse_explore(self, cl):
        """Browse explore page"""
        try:
            medias = cl.timeline_feed()[:5]
            for media in medias:
                time.sleep(random.randint(20, 60))
        except Exception as e:
            self.logger.debug(f"Browse explore failed: {e}")

    def _like_posts(self, cl, count=5):
        """Like random posts"""
        try:
            medias = cl.hashtag_medias_recent("photography", count)
            for media in medias:
                cl.media_like(media.pk)
                time.sleep(random.randint(30, 80))
        except Exception as e:
            self.logger.debug(f"Like posts failed: {e}")

    def _view_stories(self, cl, count=3):
        """View stories"""
        try:
            stories = cl.timeline_feed()[:count]
            for story in stories:
                time.sleep(random.randint(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")

    def _search_users(self, cl, count=2):
        """Search for users"""
        try:
            search_terms = ["photographer", "artist", "designer"]
            for term in random.sample(search_terms, min(count, len(search_terms))):
                cl.search_users(term)
                time.sleep(random.randint(30, 70))
        except Exception as e:
            self.logger.debug(f"Search users failed: {e}")

    def _browse_hashtags(self, cl, count=3):
        """Browse hashtags"""
        try:
            hashtags = ["art", "design", "photography"]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                time.sleep(random.randint(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")

    @rate_limit(max_calls_per_hour=100)
    def collect_usernames(self, account, source_type, source_id, amount=None):
        """
        Collect seed usernames from hashtag/location, with fallback to search.
        """
        if not account.can_scrape():
            self.logger.info(
                f"Account {account.username} cannot scrape (health: {account.health_score}%)"
            )
            return set()

        # Warm-up check
        if not account.warmed_up:
            self.logger.info(f"Warming up {account.username}")
            if not self.warm_up_account(account):
                send_alert(f"Warm-up failed for {account.username}", "warning", account)
                return set()

        amount = self._calculate_scrape_amount(account, amount)
        if amount <= 0:
            return set()

        cl = setup_client(account)
        if not cl:
            account.update_health_score(False, "scrape")
            return set()

        usernames = set()
        try:
            account.status = "scraping"
            account.save()

            # ----------------------
            # Choose method
            # ----------------------
            scrape_methods = {
                "hashtag": self._scrape_hashtag,
                "location": self._scrape_location,
            }

            if source_type in scrape_methods:
                usernames = scrape_methods[source_type](cl, source_id, amount)
                if not usernames:
                    self.logger.warning(
                        f"No results for {source_type}:{source_id}, fallback to search"
                    )
                    usernames = self._scrape_search(cl, source_id, amount)
            elif source_type == "search":
                usernames = self._scrape_search(cl, source_id, amount)
            else:
                raise ValueError(f"Invalid source_type: {source_type}")

            account.users_scraped_today += len(usernames)
            account.last_active = timezone.now()
            account.update_health_score(True, "scrape")

            self.logger.info(
                f"Collected {len(usernames)} usernames from {source_type}:{source_id}"
            )

        except ClientError as e:
            self.logger.error(f"ClientError in collect_usernames: {e}")
            account.update_health_score(False, "scrape")
            self._handle_client_error(account, e)

        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            account.update_health_score(False, "scrape")
            send_alert(
                f"Scraping error for {account.username}: {str(e)}", "error", account
            )

        finally:
            account.status = "idle"
            account.save()

        return usernames

    def _scrape_search(self, cl, query, amount):
        usernames = set()
        try:
            results = cl.search_users(query)
            for user in results[:amount]:
                if user.username:
                    usernames.add(user.username)
                time.sleep(random.uniform(20, 70))
        except Exception as e:
            self.logger.error(f"Search scrape failed for {query}: {e}")
        return usernames

    def _scrape_location(self, cl, location, amount):
        """
        Avoid location_medias_recent; instead, search users by location name.
        """
        usernames = set()
        try:
            try:
                results = cl.search_users(location)
                for user in results[:amount]:
                    if getattr(user, "username", None):
                        usernames.add(user.username)
                        time.sleep(random.uniform(30, 80))
            except Exception as e:
                self.logger.warning(f"search_users for location '{location}' failed: {e}")
        except Exception as e:
            self.logger.error(f"Location scrape failed: {e}")
        return usernames

    def _calculate_scrape_amount(self, account, requested_amount):
        """Calculate safe scraping amount based on account health and limits"""
        base_limit = settings.SCRAPING_LIMIT_PER_SOURCE

        # Adjust based on account age and health
        if account.account_age_days < 30:
            base_limit = min(base_limit, 10)
        elif account.account_age_days < 90:
            base_limit = min(base_limit, 20)

        # Health-based adjustment
        health_multiplier = account.health_score / 100
        adjusted_limit = int(base_limit * health_multiplier)

        # Respect daily limits
        remaining_daily = account.daily_scrape_limit - account.users_scraped_today

        # Final calculation
        amount = min(
            adjusted_limit,
            remaining_daily,
            requested_amount or base_limit
        )

        return max(0, amount)

    def _handle_client_error(self, account, error):
        """Handle different types of client errors"""
        error_str = str(error).lower()

        if "429" in error_str or "rate limit" in error_str:
            account.status = "rate_limited"
            account.save()
            send_alert(f"Rate limit hit for {account.username}", 'warning', account)

        elif "challenge" in error_str:
            account.status = "flagged"
            account.save()
            send_alert(f"Challenge required for {account.username}", 'error', account)

        elif "login" in error_str:
            account.login_failures += 1
            account.last_login_failure = timezone.now()
            account.save()
            send_alert(f"Login issue for {account.username}", 'error', account)

        else:
            send_alert(f"Client error for {account.username}: {error_str}", 'warning', account)

    def _scrape_hashtag(self, cl, hashtag, amount):
        """
        Avoid any media calls. Use search_users(hashtag) as a fallback.
        This is less exhaustive but avoids Media validation issues entirely.
        """
        usernames = set()
        try:
            # Primary: use search (gives user objects)
            try:
                results = cl.search_users(hashtag)
                for user in results[:amount]:
                    if getattr(user, "username", None):
                        usernames.add(user.username)
                        time.sleep(random.uniform(40, 80))
            except Exception as e:
                self.logger.warning(f"search_users for #{hashtag} failed: {e}")

            # If not enough results, try alternate query terms
            if len(usernames) < amount:
                alt_queries = [hashtag.replace("_", " "), hashtag + " official"]
                for q in alt_queries:
                    if len(usernames) >= amount:
                        break
                    try:
                        results = cl.search_users(q)
                        for user in results:
                            if getattr(user, "username", None) and len(usernames) < amount:
                                usernames.add(user.username)
                                time.sleep(random.uniform(50, 100))
                    except Exception as e:
                        self.logger.debug(f"fallback search '{q}' failed: {e}")

        except Exception as e:
            self.logger.error(f"Failed to scrape hashtag {hashtag}: {e}")
        return usernames

    def _scrape_followers(self, cl, username, amount=50):
        usernames = set()
        try:
            user_id = cl.user_id_from_username(username)
            followers = cl.user_followers(user_id, amount)
            usernames = {u.username for u in followers.values()}
        except Exception as e:
            self.logger.error(f"Failed to scrape followers of {username}: {e}")
        return usernames

    def _scrape_following(self, cl, username, amount=50):
        usernames = set()
        try:
            user_id = cl.user_id_from_username(username)
            following = cl.user_following(user_id, amount)
            usernames = {u.username for u in following.values()}
        except Exception as e:
            self.logger.error(f"Failed to scrape following of {username}: {e}")
        return usernames

    def _scrape_likers(self, cl, media_id, amount=50):
        usernames = set()
        try:
            likers = cl.media_likers(media_id)
            usernames = {u.username for u in likers[:amount]}
        except Exception as e:
            self.logger.error(f"Failed to scrape likers for {media_id}: {e}")
        return usernames

    def _scrape_commenters(self, cl, media_id, amount=50):
        usernames = set()
        try:
            comments = cl.media_comments(media_id, amount)
            usernames = {c.user.username for c in comments if c.user}
        except Exception as e:
            self.logger.error(f"Failed to scrape commenters for {media_id}: {e}")
        return usernames

    def _scrape_tags(self, cl, media_id, amount=None):
        """Scrape users tagged in a post"""
        usernames = set()
        try:
            media = cl.media_info(media_id)
            for tag in media.usertags:
                if tag.user and tag.user.username:
                    usernames.add(tag.user.username)
            time.sleep(random.uniform(60, 80))
        except Exception as e:
            self.logger.error(f"Failed to scrape tags for {media_id}: {e}")
        return usernames

    def _validate_hashtag(self, cl, hashtag):
        """Validate if hashtag exists and is accessible"""
        try:
            info = cl.hashtag_info(hashtag)
            return info.media_count > 0
        except Exception as e:
            self.logger.warning(f"Invalid hashtag {hashtag}: {e}")
            return False

    def _should_skip_media(self, media):
        """Check if media should be skipped"""
        try:
            # Skip if clips_metadata has invalid original_sound_info
            if hasattr(media, 'clips_metadata') and media.clips_metadata:
                if getattr(media.clips_metadata, 'original_sound_info', None) is None:
                    return True
            return False
        except:
            return True

    def _extract_username_safely(self, cl, user_pk, fallback_username):
        """Safely extract username with fallback methods"""
        try:
            # Primary method: user_info_v1
            user = cl.user_info_v1(user_pk)
            return user.username
        except KeyError as ke:
            if "'pinned_channels_info'" in str(ke):
                try:
                    # Fallback: GQL method
                    user = cl.user_info_by_username_gql(fallback_username)
                    return user.username
                except:
                    return fallback_username
            raise ke
        except Exception:
            return fallback_username

    def store_users_enhanced(self, usernames, account, source_type, source_value):
        """Enhanced user storage with detailed metadata and logging"""
        cl = setup_client(account)
        if not cl:
            self.logger.error(f"Failed to setup client for account {account.username}")
            return

        stored_count = 0
        skipped_count = 0

        for username in usernames:
            try:
                self.logger.info(f"Processing user: {username}")
                user_data = self._get_user_detailed_info(cl, username)

                if not user_data:
                    # Save minimal user with failure_reason
                    scraped_user, created = ScrapedUser.objects.get_or_create(
                        username=username,
                        defaults={
                            "user_id": None,
                            "biography": "",
                            "follower_count": 0,
                            "following_count": 0,
                            "post_count": 0,
                            "is_active": False,
                            "account": account,
                            "source_type": source_type,
                            "source_value": source_value,
                            "failure_reason": "Failed to fetch detailed info",
                        },
                    )
                    if created:
                        self.logger.warning(f"Stored minimal user: {username} (failed details)")
                    else:
                        scraped_user.failure_reason = "Failed to fetch detailed info"
                        scraped_user.scraped_at = timezone.now()
                        scraped_user.save()
                        self.logger.warning(f"Updated existing failed user: {username}")

                    skipped_count += 1
                    continue

                # Create or update with full user_data
                scraped_user, created = ScrapedUser.objects.get_or_create(
                    username=username,
                    defaults={
                        "user_id": user_data.get("user_id"),
                        "biography": user_data.get("biography"),
                        "follower_count": user_data.get("follower_count", 0),
                        "following_count": user_data.get("following_count", 0),
                        "post_count": user_data.get("post_count", 0),
                        "last_post_date": user_data.get("last_post_date"),
                        "is_active": user_data.get("is_active", True),
                        "account": account,
                        "source_type": source_type,
                        "source_value": source_value,
                        "failure_reason": None,
                    },
                )

                if created:
                    self.logger.info(f"Stored new user: {username}")
                    print(f"Stored new user: {username}")
                    stored_count += 1
                else:
                    # Update existing user
                    for key, value in user_data.items():
                        if key != "user_id":
                            setattr(scraped_user, key, value)
                    scraped_user.failure_reason = None
                    scraped_user.scraped_at = timezone.now()
                    scraped_user.save()
                    print(f"Updated existing user: {username}")

                time.sleep(random.uniform(60, 80))

                # occasional longer pause
                if stored_count % 50 == 0 and stored_count > 0:
                    time.sleep(random.uniform(300, 900))

            except Exception as e:
                self.logger.error(f"Failed to store user {username}: {e}")
                # Save user with error as failure_reason
                ScrapedUser.objects.update_or_create(
                    username=username,
                    defaults={
                        "user_id": None,
                        "biography": "",
                        "follower_count": 0,
                        "following_count": 0,
                        "post_count": 0,
                        "is_active": False,
                        "account": account,
                        "source_type": source_type,
                        "source_value": source_value,
                        "failure_reason": str(e),
                    },
                )
                skipped_count += 1

        self.logger.info(f"Finished storing users: Stored={stored_count}, Skipped={skipped_count}")

    def _get_user_detailed_info(self, cl, username):
        """Get detailed user information WITHOUT requesting medias"""
        try:
            user_id = cl.user_id_from_username(username)
            # Prefer user_info_by_username which is often more robust
            try:
                user_info = cl.user_info_by_username(username)
            except Exception:
                user_info = cl.user_info_v1(user_id)

            # Don't fetch recent posts — media calls removed
            last_post_date = None
            is_active = True if user_info.media_count and user_info.media_count > 0 else False

            return {
                'user_id': str(user_id),
                'biography': getattr(user_info, 'biography', '') or '',
                'follower_count': getattr(user_info, 'follower_count', 0),
                'following_count': getattr(user_info, 'following_count', 0),
                'post_count': getattr(user_info, 'media_count', 0),
                'last_post_date': last_post_date,
                'is_active': is_active,
            }

        except (UserNotFound, PrivateError):
            return None
        except Exception as e:
            self.logger.error(f"Error getting user info for {username}: {e}")
            return None