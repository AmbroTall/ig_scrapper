import logging
import time
import random
from django.utils import timezone
from django.conf import settings
from instagrapi.exceptions import ClientError, UserNotFound, PrivateError, RateLimitError
from .models import ScrapedUser
from .utils import setup_client, send_alert

# Rate limiting decorator
def rate_limit(max_calls_per_hour=60):
    def decorator(func):
        def wrapper(self, account, *args, **kwargs):
            now = timezone.now()
            if (now - account.hour_reset).total_seconds() > 3600:
                account.actions_this_hour = 0
                account.hour_reset = now
                account.save()
            if account.actions_this_hour >= max_calls_per_hour:
                account.status = "rate_limited"
                account.save()
                send_alert(f"Rate limit reached for {account.username}", "warning", account)
                raise RateLimitError("Rate limit reached for this account")
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
        """Warm up account with human-like activities"""
        cl = setup_client(account)  # Removed proxy
        if not cl:
            return False
        try:
            account.status = "warming_up"
            account.save()
            warmup_activities = [
                lambda: self._browse_explore(cl),
                lambda: self._like_posts(cl, count=random.randint(3, 7)),
                lambda: self._view_stories(cl, count=random.randint(2, 5)),
                lambda: self._search_users(cl, count=random.randint(1, 3)),
                lambda: self._browse_hashtags(cl, count=random.randint(2, 4)),
            ]
            max_level = min(account.warmup_level + 1, len(warmup_activities))
            for i in range(max_level):
                warmup_activities[i]()
                time.sleep(random.uniform(30, 90))
            account.warmup_level = max_level
            account.warmed_up = max_level >= 3
            account.last_active = timezone.now()
            account.update_health_score(True, 'warmup')
            self.logger.info(f"Warmed up {account.username} to level {max_level}")
            return True
        except ClientError as e:
            self.logger.error(f"Warm-up failed for {account.username}: {e}")
            account.update_health_score(False, 'warmup')
            return False
        finally:
            account.status = "idle"
            account.save()

    def _browse_explore(self, cl):
        try:
            cl.timeline_feed()[:5]
            time.sleep(random.uniform(20, 60))
        except Exception as e:
            self.logger.debug(f"Browse explore failed: {e}")

    def _like_posts(self, cl, count=5):
        try:
            medias = cl.hashtag_medias_recent("photography", count)
            for media in medias:
                cl.media_like(media.pk)
                time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"Like posts failed: {e}")

    def _view_stories(self, cl, count=3):
        try:
            stories = cl.timeline_feed()[:count]
            time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")

    def _search_users(self, cl, count=2):
        try:
            search_terms = ["photographer", "artist", "designer"]
            for term in random.sample(search_terms, min(count, len(search_terms))):
                cl.search_users(term)
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Search users failed: {e}")

    def _browse_hashtags(self, cl, count=3):
        try:
            hashtags = ["art", "design", "photography"]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")

    @rate_limit(max_calls_per_hour=100)
    def collect_usernames(self, account, source_type, source_id, amount=None):
        """Collect usernames by scraping likers of top hashtag posts"""
        if not account.can_scrape():
            self.logger.info(f"Account {account.username} cannot scrape (health: {account.health_score}%)")
            return set()
        if not account.warmed_up:
            self.logger.info(f"Warming up {account.username}")
            if not self.warm_up_account(account):
                send_alert(f"Warm-up failed for {account.username}", "warning", account)
                return set()
        amount = self._calculate_scrape_amount(account, amount)
        if amount <= 0:
            return set()
        cl = setup_client(account)  # Removed proxy
        if not cl:
            account.update_health_score(False, "scrape")
            return set()
        usernames = set()
        try:
            account.status = "scraping"
            account.save()
            if source_type == "hashtag":
                usernames = self._scrape_hashtag_likers(cl, source_id, amount)
                if len(usernames) < amount * 0.5:  # Fallback if yield is too low
                    self.logger.warning(f"Low yield ({len(usernames)}) for hashtag:{source_id}, falling back to search")
                    usernames |= self._scrape_search(cl, source_id, amount - len(usernames))
            elif source_type == "search":
                usernames = self._scrape_search(cl, source_id, amount)
            else:
                raise ValueError(f"Invalid source_type: {source_type}")
            account.users_scraped_today += len(usernames)
            account.last_active = timezone.now()
            account.update_health_score(True, "scrape")
            self.logger.info(f"Collected {len(usernames)} usernames from {source_type}:{source_id}")
        except RateLimitError:
            account.status = "rate_limited"
            account.save()
            send_alert(f"Rate limit hit for {account.username}", "warning", account)
            time.sleep(random.uniform(300, 600))
        except ClientError as e:
            self.logger.error(f"ClientError in collect_usernames: {e}")
            account.update_health_score(False, "scrape")
            self._handle_client_error(account, e)
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            account.update_health_score(False, "scrape")
            send_alert(f"Scraping error for {account.username}: {str(e)}", "error", account)
        finally:
            account.status = "idle"
            account.save()
        return usernames

    def _scrape_hashtag_likers(self, cl, hashtag, amount):
        """Scrape likers of top posts for a hashtag with batch processing"""
        usernames = set()
        try:
            max_posts = 10  # Max posts to try
            min_likers_threshold = 20  # Skip posts with fewer likers
            num_posts = random.randint(3, 7)  # Initial post count
            top_medias = cl.hashtag_medias_top(hashtag, num_posts)
            likers_per_post = min(200, max(100, int(amount / len(top_medias))))  # Dynamic likers per post
            posts_processed = 0
            for media in top_medias:
                try:
                    likers = []
                    chunk_size = 50
                    total_likers = 0
                    while total_likers < likers_per_post and total_likers < amount - len(usernames):
                        try:
                            likers_chunk = cl.media_likers(media.pk)
                            if len(likers_chunk) < min_likers_threshold:
                                self.logger.warning(f"Skipping media {media.pk}: only {len(likers_chunk)} likers")
                                break
                            likers.extend(likers_chunk[:chunk_size])
                            total_likers += len(likers_chunk)
                            time.sleep(random.uniform(10, 20))  # Delay between chunks
                        except RateLimitError:
                            self.logger.warning(f"Rate limit hit for media {media.pk}")
                            send_alert(f"Rate limit hit for media {media.pk} on {hashtag}", "warning")
                            time.sleep(random.uniform(300, 600))
                            break
                        except Exception as e:
                            self.logger.warning(f"Failed to fetch likers for media {media.pk}: {e}")
                            break
                    if total_likers >= min_likers_threshold:
                        for liker in likers[:likers_per_post]:
                            usernames.add(liker.username)
                        self.logger.info(f"Scraped {len(likers[:likers_per_post])} likers from media {media.pk}")
                    posts_processed += 1
                    time.sleep(random.uniform(120, 300))  # Long delay between posts
                    if random.random() < 0.3:  # 30% chance of warm-up
                        self._like_posts(cl, count=1)
                        time.sleep(random.uniform(30, 60))
                    if len(usernames) >= amount:
                        break
                except Exception as e:
                    self.logger.warning(f"Failed to process media {media.pk}: {e}")
                # Fetch more posts if needed
                if len(usernames) < amount * 0.5 and posts_processed >= num_posts and num_posts < max_posts:
                    self.logger.info(f"Low yield, fetching {max_posts - num_posts} more posts")
                    num_posts = max_posts
                    top_medias += cl.hashtag_medias_top(hashtag, max_posts - posts_processed)[posts_processed:]
            if not usernames and not self._validate_hashtag(cl, hashtag):
                self.logger.warning(f"Invalid or inaccessible hashtag: {hashtag}")
        except Exception as e:
            self.logger.error(f"Failed to scrape hashtag {hashtag}: {e}")
        return usernames

    def _scrape_search(self, cl, query, amount):
        """Fallback search method"""
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

    def _calculate_scrape_amount(self, account, requested_amount):
        """Calculate safe scraping amount"""
        base_limit = getattr(settings, 'SCRAPING_LIMIT_PER_SOURCE', 1000)
        if account.account_age_days < 30:
            base_limit = min(base_limit, 200)
        elif account.account_age_days < 90:
            base_limit = min(base_limit, 500)
        health_multiplier = account.health_score / 100
        adjusted_limit = int(base_limit * health_multiplier)
        remaining_daily = account.daily_scrape_limit - account.users_scraped_today
        amount = min(adjusted_limit, remaining_daily, requested_amount or base_limit)
        return max(0, amount)

    def _handle_client_error(self, account, error):
        """Handle client errors with exponential backoff"""
        error_str = str(error).lower()
        backoff_times = [300, 600, 1200]  # 5m, 10m, 20m
        if "429" in error_str or "rate limit" in error_str:
            account.status = "rate_limited"
            account.save()
            send_alert(f"Rate limit hit for {account.username}", "warning", account)
            time.sleep(random.choice(backoff_times))
        elif "challenge" in error_str:
            account.status = "flagged"
            account.save()
            send_alert(f"Challenge required for {account.username}", "error", account)
        elif "login" in error_str:
            account.login_failures += 1
            account.last_login_failure = timezone.now()
            account.save()
            send_alert(f"Login issue for {account.username}", "error", account)
        else:
            send_alert(f"Client error for {account.username}: {error_str}", "warning", account)

    def _validate_hashtag(self, cl, hashtag):
        """Validate if hashtag exists and is accessible"""
        try:
            info = cl.hashtag_info(hashtag)
            return info.media_count > 0
        except Exception as e:
            self.logger.warning(f"Invalid hashtag {hashtag}: {e}")
            return False

    def store_users_enhanced(self, usernames, account, source_type, source_value):
        """Store minimal user data in bulk and queue detailed enrichment"""
        users_to_create = []
        skipped_count = 0
        for username in usernames:
            try:
                # Store minimal data
                users_to_create.append(ScrapedUser(
                    username=username,
                    account=account,
                    source_type=source_type,
                    source_value=source_value,
                    is_active=True,  # Assume active until proven otherwise
                    details_fetched=False  # Mark as not enriched
                ))
            except Exception as e:
                self.logger.error(f"Failed to prepare user {username}: {e}")
                skipped_count += 1

        if users_to_create:
            ScrapedUser.objects.bulk_create(users_to_create, ignore_conflicts=True)
            self.logger.info(f"Stored {len(users_to_create)} users, skipped {skipped_count}")
            # Queue enrichment task
            # enrich_user_details_task.delay(account.id, source_type, source_value)
        else:
            self.logger.info(f"No users stored, skipped {skipped_count}")

    def _get_user_detailed_info(self, cl, username):
        """Get user info without media calls"""
        try:
            user_id = cl.user_id_from_username(username)
            user_info = cl.user_info_by_username(username)
            last_post_date = None
            is_active = user_info.media_count > 0
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