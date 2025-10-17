import logging
import time
import random

from django.db import transaction
from django.utils import timezone
from django.core.cache import cache
from django.conf import settings
from instagrapi.exceptions import ClientError, UserNotFound, PrivateError, RateLimitError
from instagrapi.mixins.challenge import ChallengeChoice
from pydantic import ValidationError

from .models import ScrapedUser, ProcessedMedia
from .utils import setup_client, send_alert, challenge_code_handler


# Rate limiting decorator (unchanged)
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
                # lambda: self._browse_explore(cl),
                lambda: self._like_posts(cl, count=random.randint(3, 7)),
                # lambda: self._view_stories(cl, count=random.randint(2, 5)),
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
            send_alert(f"Warmed up {account.username} to level {max_level}", "info", account)
            return True
        except ClientError as e:
            self.logger.error(f"Warm-up failed for {account.username}: {e}")
            send_alert(f"Warm-up failed for {account.username}: {e}", "error", account)
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
            send_alert(f"Browse explore failed: {e}", "info")

    def _like_posts(self, cl, count=5):
        try:
            medias = cl.hashtag_medias_recent("photography", count)
            for media in medias:
                cl.media_like(media.pk)
                time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"Like posts failed: {e}")
            send_alert(f"Like posts failed: {e}", "info")

    def _view_stories(self, cl, count=3):
        try:
            stories = cl.timeline_feed()[:count]
            time.sleep(random.uniform(30, 80))
        except Exception as e:
            self.logger.debug(f"View stories failed: {e}")
            send_alert(f"View stories failed: {e}", "info")

    def _search_users(self, cl, count=2):
        try:
            search_terms = ["photographer", "artist", "designer"]
            for term in random.sample(search_terms, min(count, len(search_terms))):
                cl.search_users(term)
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Search users failed: {e}")
            send_alert(f"Search users failed: {e}", "info")

    def _browse_hashtags(self, cl, count=3):
        try:
            hashtags = ["art", "design", "photography"]
            for hashtag in random.sample(hashtags, min(count, len(hashtags))):
                cl.hashtag_info(hashtag)
                time.sleep(random.uniform(30, 70))
        except Exception as e:
            self.logger.debug(f"Browse hashtags failed: {e}")
            send_alert(f"Browse hashtags failed: {e}", "info")

    @rate_limit(max_calls_per_hour=100)
    def collect_usernames(self, account, source_type, source_id, amount=None):
        """Collect usernames by scraping likers of top hashtag posts"""
        usernames = set()
        try:
            if not account.can_scrape():
                self.logger.info(f"Account {account.username} cannot scrape (health: {account.health_score}%)")
                send_alert(f"Account {account.username} cannot scrape (health: {account.health_score}%)", "info",
                           account)
                return usernames

            if not account.warmed_up:
                self.logger.info(f"Warming up {account.username}")
                send_alert(f"Warming up {account.username}", "info", account)
                if not self.warm_up_account(account):
                    send_alert(f"Warm-up failed for {account.username}", "warning", account)
                    return usernames

            amount = self._calculate_scrape_amount(account, amount)
            if amount <= 0:
                return usernames

            cl = setup_client(account)
            if not cl:
                with transaction.atomic():
                    account.update_health_score(False, "scrape")
                    account.status = "idle"
                    account.save()
                return usernames

            try:
                with transaction.atomic():
                    account.status = "scraping"
                    account.save()

                if source_type == "hashtag":
                    # Saving is handled within _scrape_hashtag_likers
                    self._scrape_hashtag_likers(cl, source_id, account, source_type)
                else:
                    usernames = self._scrape_search(cl, source_id, amount, account)
                    if usernames:
                        with transaction.atomic():
                            account.last_active = timezone.now()
                            account.update_health_score(True, "scrape")
                            account.status = "idle"
                            account.save()
                        self.logger.info(
                            f"Collected and saved {len(usernames)} usernames from {source_type}:{source_id}")
                        cache_key = f"alert_collect_usernames_{source_type}_{source_id}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Collected and saved {len(usernames)} usernames from {source_type}:{source_id}",
                                       "info", account)
                            cache.set(cache_key, True, timeout=60)
            except RateLimitError:
                with transaction.atomic():
                    account.status = "rate_limited"
                    account.save()
                send_alert(f"Rate limit hit for {account.username}", "warning", account)
                time.sleep(random.uniform(300, 600))
            except ClientError as e:
                self.logger.error(f"ClientError in collect_usernames: {e}")
                send_alert(f"ClientError in collect_usernames: {e}", "error", account)
                with transaction.atomic():
                    account.update_health_score(False, "scrape")
                    account.status = "idle"
                    account.save()
                self._handle_client_error(account, e)
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}", exc_info=True)
                send_alert(f"Unexpected error: {str(e)}", "error", account)
                with transaction.atomic():
                    account.update_health_score(False, "scrape")
                    account.status = "idle"
                    account.save()
                send_alert(f"Scraping error for {account.username}: {str(e)}", "error", account)

        except Exception as e:
            self.logger.error(f"Unexpected outer error in collect_usernames: {e}")
            send_alert(f"Unexpected outer error in collect_usernames: {e}", "error", account)
            with transaction.atomic():
                account.status = "idle"
                account.save()

        return usernames

    def _scrape_hashtag_likers(self, cl, hashtag, account, source_type="hashtag"):
        """Scrape likers, commenters, and post owners of top posts for a hashtag, tracking processed media in database."""
        usernames = set()  # Stores tuples of (username, user_id)
        batch_size = getattr(settings, 'SCRAPING_BATCH_SIZE', 1000)
        total_likers_processed = 0
        total_commenters_processed = 0
        total_posts_processed = 0
        max_posts = 100
        num_posts = random.randint(20, 50)
        max_likers_per_media = 50  # Fixed number of likers to fetch per media
        max_comments_per_media = 50  # Fixed number of comments to fetch per media

        try:
            # Validate hashtag
            try:
                hashtag_info = cl.hashtag_info(hashtag)
                self.logger.info(f"Hashtag {hashtag} has {hashtag_info.media_count} posts")
                cache_key = f"alert_hashtag_info_{hashtag}_{account.id}"
                if not cache.get(cache_key):
                    send_alert(f"Hashtag {hashtag} has {hashtag_info.media_count} posts", "info", account)
                    cache.set(cache_key, True, timeout=60)
                if hashtag_info.media_count == 0:
                    self.logger.warning(f"Hashtag {hashtag} has no posts, falling back to search")
                    send_alert(f"Hashtag {hashtag} has no posts, falling back to search", "warning", account)
                    return self._scrape_search(cl, hashtag, amount=1000, account=account)
            except ClientError as e:
                self.logger.error(f"Failed to fetch hashtag info for {hashtag}: {e}")
                send_alert(f"Failed to fetch hashtag info for {hashtag}: {e}", "error", account)
                self.logger.warning(f"Invalid hashtag {hashtag}, falling back to search")
                send_alert(f"Invalid hashtag {hashtag}, falling back to search", "warning", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)
            except Exception as e:
                self.logger.error(f"Unexpected error fetching hashtag info for {hashtag}: {e}")
                send_alert(f"Unexpected error fetching hashtag info for {hashtag}: {e}", "error", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)

            # Fetch top posts
            try:
                top_medias = cl.hashtag_medias_top(hashtag, amount=num_posts)
                random.shuffle(top_medias)
                top_medias = top_medias[:max_posts]
                if not top_medias:
                    self.logger.warning(f"No media found for hashtag {hashtag}, falling back to search")
                    send_alert(f"No media found for hashtag {hashtag}, falling back to search", "warning", account)
                    return self._scrape_search(cl, hashtag, amount=1000, account=account)
                self.logger.info(f"Scraping users for hashtag {hashtag}, {len(top_medias)} posts (top)")
                cache_key = f"alert_scraping_users_{hashtag}_{account.id}"
                if not cache.get(cache_key):
                    send_alert(f"Scraping users for hashtag {hashtag}, {len(top_medias)} posts (top)", "info", account)
                    cache.set(cache_key, True, timeout=60)
            except ValidationError as e:
                self.logger.error(f"Pydantic validation error fetching media for {hashtag}: {e}")
                send_alert(f"Pydantic validation error fetching media for {hashtag}: {e}", "error", account)
                self.logger.warning(f"Unable to fetch media for {hashtag}, falling back to search")
                send_alert(f"Unable to fetch media for {hashtag}, falling back to search", "warning", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)
            except ClientError as e:
                self.logger.error(f"Failed to fetch media for {hashtag}: {e}")
                send_alert(f"Failed to fetch media for {hashtag}: {e}", "error", account)
                self.logger.warning(f"No media for {hashtag}, falling back to search")
                send_alert(f"No media for {hashtag}, falling back to search", "warning", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)
            except Exception as e:
                self.logger.error(f"Unexpected error fetching media for {hashtag}: {e}")
                send_alert(f"Unexpected error fetching media for {hashtag}: {e}", "error", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)

            posts_processed = 0
            for media in top_medias:
                # Check if media is already processed
                if ProcessedMedia.objects.filter(media_id=media.pk, hashtag=hashtag, account=account).exists():
                    self.logger.info(f"Skipping already processed media {media.pk} for hashtag {hashtag}")
                    cache_key = f"alert_skip_media_{media.pk}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Skipping already processed media {media.pk} for hashtag {hashtag}", "info",
                                   account)
                        cache.set(cache_key, True, timeout=60)
                    continue

                try:
                    # Collect post owner
                    try:
                        if media.user and media.user.username:
                            usernames.add((media.user.username, media.user.pk))
                            self.logger.info(
                                f"Added post owner {media.user.username} (ID: {media.user.pk}) for media {media.pk}")
                            cache_key = f"alert_post_owner_{media.pk}_{account.id}"
                            if not cache.get(cache_key):
                                send_alert(
                                    f"Added post owner {media.user.username} (ID: {media.user.pk}) for media {media.pk}",
                                    "info", account)
                                cache.set(cache_key, True, timeout=60)
                    except Exception as e:
                        self.logger.warning(f"Failed to get post owner for media {media.pk}: {e}")
                        send_alert(f"Failed to get post owner for media {media.pk}: {e}", "warning", account)

                    # Save batch if needed (after post owner)
                    if len(usernames) >= batch_size:
                        try:
                            with transaction.atomic():
                                saved_count = self.store_users_enhanced(list(usernames), account, source_type, hashtag)
                                account.users_scraped_today += saved_count
                                account.save()
                                self.logger.info(
                                    f"Saved batch of {saved_count} users for hashtag {hashtag} (post owner)")
                                cache_key = f"alert_save_batch_post_owner_{hashtag}_{account.id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Saved batch of {saved_count} users for hashtag {hashtag} (post owner)",
                                               "info", account)
                                    cache.set(cache_key, True, timeout=60)
                            usernames.clear()  # Clear only after successful save
                            time.sleep(random.uniform(5, 15))  # Delay after batch save
                        except Exception as e:
                            self.logger.error(f"Failed to save batch for hashtag {hashtag} (post owner): {e}")
                            send_alert(f"Failed to save batch for hashtag {hashtag} (post owner): {e}", "error",
                                       account)
                            continue  # Don't clear usernames, try next batch

                    # Collect likers
                    try:
                        likers = cl.media_likers(media.pk)
                        media_likers_count = len(likers)
                        total_likers_processed += media_likers_count
                        self.logger.info(
                            f"Fetched {media_likers_count} likers for media {media.pk}, "
                            f"total for this media {media_likers_count}"
                        )
                        cache_key = f"alert_likers_fetched_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(
                                f"Fetched {media_likers_count} likers for media {media.pk}, "
                                f"total for this media {media_likers_count}", "info", account
                            )
                            cache.set(cache_key, True, timeout=60)

                        # Add likers to usernames
                        for liker in likers:
                            if liker.username and liker.pk:
                                usernames.add((liker.username, liker.pk))

                        # Save batch if needed (after likers)
                        if len(usernames) >= batch_size:
                            try:
                                with transaction.atomic():
                                    saved_count = self.store_users_enhanced(list(usernames), account, source_type,
                                                                            hashtag)
                                    account.users_scraped_today += saved_count
                                    account.save()
                                    self.logger.info(
                                        f"Saved batch of {saved_count} users for hashtag {hashtag} (likers)")
                                    cache_key = f"alert_save_batch_likers_{hashtag}_{account.id}"
                                    if not cache.get(cache_key):
                                        send_alert(
                                            f"Saved batch of {saved_count} users for hashtag {hashtag} (likers)",
                                            "info", account)
                                        cache.set(cache_key, True, timeout=60)
                                usernames.clear()  # Clear only after successful save
                                time.sleep(random.uniform(5, 15))  # Delay after batch save
                            except Exception as e:
                                self.logger.error(f"Failed to save batch for hashtag {hashtag} (likers): {e}")
                                send_alert(f"Failed to save batch for hashtag {hashtag} (likers): {e}", "error",
                                           account)
                                continue  # Don't clear usernames, try next batch
                    except RateLimitError:
                        self.logger.warning(f"Rate limit hit for media {media.pk} (likers)")
                        send_alert(f"Rate limit hit for media {media.pk} on {hashtag} (likers)", "warning", account)
                        account.status = "rate_limited"
                        account.task_id = None
                        account.save()
                        time.sleep(random.uniform(60, 120))
                        continue
                    except ClientError as e:
                        self.logger.warning(f"Failed to fetch likers for media {media.pk}: {e}")
                        send_alert(f"Failed to fetch likers for media {media.pk}: {e}", "warning", account)
                        if "challenge_required" in str(e).lower():
                            code = challenge_code_handler(account.username, ChallengeChoice.SMS)
                            if code:
                                try:
                                    cl.challenge_resolve(code)
                                    self.logger.info(f"Challenge resolved for {account.username}, retrying likers")
                                    send_alert(f"Challenge resolved for {account.username}, retrying likers", "info",
                                               account)
                                    continue
                                except Exception as ce:
                                    self.logger.warning(f"Challenge resolution failed for {account.username}: {ce}")
                                    send_alert(f"Challenge resolution failed for {account.username}: {ce}", "warning",
                                               account)
                            account.status = "error"
                            account.login_failures += 1
                            account.last_login_failure = timezone.now()
                            account.task_id = None
                            account.save()
                            self.logger.warning(f"Challenge required for {account.username}, skipping media")
                            send_alert(f"Challenge required for {account.username}, skipping media", "warning", account)
                        continue
                    except ValidationError as e:
                        self.logger.warning(f"Pydantic validation error fetching likers for media {media.pk}: {e}")
                        send_alert(f"Pydantic validation error fetching likers for media {media.pk}: {e}", "warning",
                                   account)
                        continue
                    except Exception as e:
                        self.logger.warning(f"Unexpected error fetching likers for media {media.pk}: {e}")
                        send_alert(f"Unexpected error fetching likers for media {media.pk}: {e}", "warning", account)
                        continue

                    # Collect commenters
                    try:
                        comments = cl.media_comments(media.pk, amount=max_comments_per_media)
                        media_commenters_count = len(comments)
                        total_commenters_processed += media_commenters_count
                        self.logger.info(
                            f"Fetched {media_commenters_count} comments for media {media.pk}, "
                            f"total for this media {media_commenters_count}"
                        )
                        cache_key = f"alert_comments_fetched_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(
                                f"Fetched {media_commenters_count} comments for media {media.pk}, "
                                f"total for this media {media_commenters_count}", "info", account
                            )
                            cache.set(cache_key, True, timeout=60)

                        # Add commenters to usernames
                        for comment in comments:
                            if comment.user.username and comment.user.pk:
                                usernames.add((comment.user.username, comment.user.pk))

                        # Save batch if needed (after commenters)
                        if len(usernames) >= batch_size:
                            try:
                                with transaction.atomic():
                                    saved_count = self.store_users_enhanced(list(usernames), account, source_type,
                                                                            hashtag)
                                    account.users_scraped_today += saved_count
                                    account.save()
                                    self.logger.info(
                                        f"Saved batch of {saved_count} users for hashtag {hashtag} (commenters)")
                                    cache_key = f"alert_save_batch_commenters_{hashtag}_{account.id}"
                                    if not cache.get(cache_key):
                                        send_alert(
                                            f"Saved batch of {saved_count} users for hashtag {hashtag} (commenters)",
                                            "info", account)
                                        cache.set(cache_key, True, timeout=60)
                                usernames.clear()  # Clear only after successful save
                                time.sleep(random.uniform(5, 15))  # Delay after batch save
                            except Exception as e:
                                self.logger.error(f"Failed to save batch for hashtag {hashtag} (commenters): {e}")
                                send_alert(f"Failed to save batch for hashtag {hashtag} (commenters): {e}", "error",
                                           account)
                                continue  # Don't clear usernames, try next batch
                    except RateLimitError:
                        self.logger.warning(f"Rate limit hit for media {media.pk} (comments)")
                        send_alert(f"Rate limit hit for media {media.pk} on {hashtag} (comments)", "warning", account)
                        account.status = "rate_limited"
                        account.task_id = None
                        account.save()
                        time.sleep(random.uniform(60, 120))
                        continue
                    except ClientError as e:
                        self.logger.warning(f"Failed to fetch comments for media {media.pk}: {e}")
                        send_alert(f"Failed to fetch comments for media {media.pk}: {e}", "warning", account)
                        if "challenge_required" in str(e).lower():
                            code = challenge_code_handler(account.username, ChallengeChoice.SMS)
                            if code:
                                try:
                                    cl.challenge_resolve(code)
                                    self.logger.info(f"Challenge resolved for {account.username}, retrying comments")
                                    send_alert(f"Challenge resolved for {account.username}, retrying comments", "info",
                                               account)
                                    continue
                                except Exception as ce:
                                    self.logger.warning(f"Challenge resolution failed for {account.username}: {ce}")
                                    send_alert(f"Challenge resolution failed for {account.username}: {ce}", "warning",
                                               account)
                            account.status = "error"
                            account.login_failures += 1
                            account.last_login_failure = timezone.now()
                            account.task_id = None
                            account.save()
                            self.logger.warning(f"Challenge required for {account.username}, skipping media")
                            send_alert(f"Challenge required for {account.username}, skipping media", "warning", account)
                        continue
                    except ValidationError as e:
                        self.logger.warning(f"Pydantic validation error fetching comments for media {media.pk}: {e}")
                        send_alert(f"Pydantic validation error fetching comments for media {media.pk}: {e}", "warning",
                                   account)
                        continue
                    except Exception as e:
                        self.logger.warning(f"Unexpected error fetching comments for media {media.pk}: {e}")
                        send_alert(f"Unexpected error fetching comments for media {media.pk}: {e}", "warning", account)
                        continue

                    # Add likers and commenters to usernames (already done above, no need to repeat)
                    total_likers_processed += media_likers_count
                    total_commenters_processed += media_commenters_count
                    self.logger.info(
                        f"Processed media {media.pk} for hashtag {hashtag}: "
                        f"likers: {media_likers_count}, commenters: {media_commenters_count}, "
                        f"total unique users {len(usernames)}"
                    )
                    cache_key = f"alert_processed_media_{media.pk}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(
                            f"Processed media {media.pk} for hashtag {hashtag}: "
                            f"likers: {media_likers_count}, commenters: {media_commenters_count}, "
                            f"total unique users {len(usernames)}", "info", account
                        )
                        cache.set(cache_key, True, timeout=60)

                    # Save batch if needed (after all users for this post)
                    if len(usernames) >= batch_size:
                        try:
                            with transaction.atomic():
                                saved_count = self.store_users_enhanced(list(usernames), account, source_type, hashtag)
                                account.users_scraped_today += saved_count
                                account.save()
                                self.logger.info(f"Saved batch of {saved_count} users for hashtag {hashtag} (post)")
                                cache_key = f"alert_save_batch_post_{hashtag}_{account.id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Saved batch of {saved_count} users for hashtag {hashtag} (post)",
                                               "info", account)
                                    cache.set(cache_key, True, timeout=60)
                            usernames.clear()  # Clear only after successful save
                            time.sleep(random.uniform(5, 15))  # Delay after batch save
                        except Exception as e:
                            self.logger.error(f"Failed to save batch for hashtag {hashtag} (post): {e}")
                            send_alert(f"Failed to save batch for hashtag {hashtag} (post): {e}", "error", account)
                            continue  # Don't clear usernames, try next batch

                    # Mark media as processed
                    try:
                        ProcessedMedia.objects.create(
                            media_id=media.pk,
                            hashtag=hashtag,
                            account=account,
                            source_type=source_type
                        )
                        self.logger.info(f"Marked media {media.pk} as processed for hashtag {hashtag}")
                        cache_key = f"alert_marked_media_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Marked media {media.pk} as processed for hashtag {hashtag}", "info", account)
                            cache.set(cache_key, True, timeout=60)
                    except Exception as e:
                        self.logger.warning(f"Failed to mark media {media.pk} as processed: {e}")
                        send_alert(f"Failed to mark media {media.pk} as processed: {e}", "warning", account)

                    posts_processed += 1
                    total_posts_processed += 1
                    self.logger.info(f"Processed post {posts_processed}/{len(top_medias)} for hashtag {hashtag}")
                    cache_key = f"alert_processed_post_{media.pk}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Processed post {posts_processed}/{len(top_medias)} for hashtag {hashtag}", "info",
                                   account)
                        cache.set(cache_key, True, timeout=60)

                    # Human-like behavior
                    if random.random() < 0.5:
                        self._like_posts(cl, count=1)
                        self.logger.info(f"Liked a post for warm-up on media {media.pk}")
                        cache_key = f"alert_like_post_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Liked a post for warm-up on media {media.pk}", "info", account)
                            cache.set(cache_key, True, timeout=60)
                        time.sleep(random.uniform(30, 60))
                    if random.random() < 0.2:
                        count = random.randint(3, 7)
                        self._like_posts(cl, count=count)
                        self.logger.info(f"Liked {count} posts for warm-up")
                        cache_key = f"alert_like_posts_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Liked {count} posts for warm-up", "info", account)
                            cache.set(cache_key, True, timeout=60)
                        time.sleep(random.uniform(20, 40))
                    if random.random() < 0.1:
                        count = random.randint(2, 4)
                        self._browse_hashtags(cl, count=count)
                        self.logger.info(f"Browsed {count} hashtags for warm-up")
                        cache_key = f"alert_browse_hashtags_{media.pk}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Browsed {count} hashtags for warm-up", "info", account)
                            cache.set(cache_key, True, timeout=60)
                        time.sleep(random.uniform(40, 80))

                    time.sleep(random.uniform(120, 300))  # Delay between posts

                except ValidationError as e:
                    self.logger.warning(f"Pydantic validation error for media {media.pk}: {e}")
                    send_alert(f"Pydantic validation error for media {media.pk}: {e}", "warning", account)
                    continue
                except Exception as e:
                    self.logger.warning(f"Failed to process media {media.pk}: {e}")
                    send_alert(f"Failed to process media {media.pk}: {e}", "warning", account)
                    continue

            # Save remaining usernames
            if usernames:
                try:
                    with transaction.atomic():
                        saved_count = self.store_users_enhanced(list(usernames), account, source_type, hashtag)
                        account.users_scraped_today += saved_count
                        account.save()
                        self.logger.info(f"Saved final batch of {saved_count} users for hashtag {hashtag}")
                        cache_key = f"alert_save_final_batch_{hashtag}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Saved final batch of {saved_count} users for hashtag {hashtag}", "info",
                                       account)
                            cache.set(cache_key, True, timeout=60)
                    time.sleep(random.uniform(5, 15))
                except Exception as e:
                    self.logger.error(f"Failed to save final batch for hashtag {hashtag}: {e}")
                    send_alert(f"Failed to save final batch for hashtag {hashtag}: {e}", "error", account)

            # Fallback if no users scraped and hashtag is invalid
            if not account.users_scraped_today and not self._validate_hashtag(cl, hashtag):
                self.logger.warning(f"Invalid or inaccessible hashtag: {hashtag}, falling back to search")
                send_alert(f"Invalid or inaccessible hashtag: {hashtag}, falling back to search", "warning", account)
                return self._scrape_search(cl, hashtag, amount=1000, account=account)

            self.logger.info(
                f"Completed scraping for hashtag {hashtag}: "
                f"{account.users_scraped_today} total users saved, "
                f"{total_likers_processed} total likers processed, "
                f"{total_commenters_processed} total commenters processed, "
                f"{total_posts_processed} total posts processed"
            )
            cache_key = f"alert_completed_scraping_{hashtag}_{account.id}"
            if not cache.get(cache_key):
                send_alert(
                    f"Completed scraping for hashtag {hashtag}: "
                    f"{account.users_scraped_today} total users saved, "
                    f"{total_likers_processed} total likers processed, "
                    f"{total_commenters_processed} total commenters processed, "
                    f"{total_posts_processed} total posts processed", "info", account
                )
                cache.set(cache_key, True, timeout=60)

            return []  # Return empty list since saving is handled internally

        except Exception as e:
            self.logger.error(f"Failed to scrape hashtag {hashtag}: {e}")
            send_alert(f"Failed to scrape hashtag {hashtag}: {e}", "error", account)
            self.logger.warning(f"General error for {hashtag}, falling back to search")
            send_alert(f"General error for {hashtag}, falling back to search", "warning", account)
            if usernames:
                try:
                    with transaction.atomic():
                        saved_count = self.store_users_enhanced(list(usernames), account, source_type, hashtag)
                        account.users_scraped_today += saved_count
                        account.save()
                        self.logger.info(f"Saved {saved_count} users before error for hashtag {hashtag}")
                        cache_key = f"alert_save_before_error_{hashtag}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(f"Saved {saved_count} users before error for hashtag {hashtag}", "info", account)
                            cache.set(cache_key, True, timeout=60)
                    time.sleep(random.uniform(5, 15))
                except Exception as e:
                    self.logger.error(f"Failed to save usernames before error for hashtag {hashtag}: {e}")
                    send_alert(f"Failed to save usernames before error for hashtag {hashtag}: {e}", "error", account)
            return self._scrape_search(cl, hashtag, amount=1000, account=account)

    def _scrape_search(self, cl, query, amount, account):
        """Fallback search method with batch saving and optimized delays"""
        usernames = set()
        batch_size = getattr(settings, 'SCRAPING_BATCH_SIZE', 1000)
        total_users_processed = 0
        try:
            self.logger.info(f"Scraping users for search query {query}, targeting {amount} users")
            cache_key = f"alert_scrape_search_start_{query}_{account.id}"
            if not cache.get(cache_key):
                send_alert(f"Scraping users for search query {query}, targeting {amount} users", "info", account)
                cache.set(cache_key, True, timeout=60)

            results = cl.search_users(query)
            for user in results:
                try:
                    if user.username and user.pk:  # Ensure user_id is available
                        usernames.add((user.username, user.pk))  # Store tuple (username, user_id)
                        total_users_processed += 1
                        self.logger.info(
                            f"Added username {user.username} (ID: {user.pk}) for query {query}, "
                            f"total unique usernames {len(usernames)}, "
                            f"total users processed {total_users_processed}"
                        )
                        cache_key = f"alert_add_user_{user.pk}_{query}_{account.id}"
                        if not cache.get(cache_key):
                            send_alert(
                                f"Added username {user.username} (ID: {user.pk}) for query {query}, "
                                f"total unique usernames {len(usernames)}, "
                                f"total users processed {total_users_processed}", "info", account
                            )
                            cache.set(cache_key, True, timeout=60)

                        # Save usernames in batches
                        if len(usernames) >= batch_size:
                            try:
                                with transaction.atomic():
                                    saved_count = self.store_users_enhanced(list(usernames), account, "search", query)
                                    account.users_scraped_today += saved_count
                                    account.save()
                                self.logger.info(f"Saved batch of {saved_count} usernames for query {query}")
                                cache_key = f"alert_save_batch_search_{query}_{account.id}"
                                if not cache.get(cache_key):
                                    send_alert(f"Saved batch of {saved_count} usernames for query {query}", "info",
                                               account)
                                    cache.set(cache_key, True, timeout=60)
                                usernames.clear()  # Clear only after successful save
                                time.sleep(random.uniform(5, 15))  # Delay after batch save
                            except Exception as e:
                                self.logger.error(f"Failed to save batch for query {query}: {e}")
                                send_alert(f"Failed to save batch for query {query}: {e}", "error", account)
                                continue  # Don't clear usernames, try next batch

                    if total_users_processed >= amount:
                        break  # Respect the amount limit

                except Exception as e:
                    self.logger.warning(f"Failed to process user for query {query}: {e}")
                    cache_key = f"alert_process_user_error_{query}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Failed to process user for query {query}: {e}", "warning", account)
                        cache.set(cache_key, True, timeout=60)
                    continue

            # Save any remaining usernames
            if usernames:
                try:
                    with transaction.atomic():
                        saved_count = self.store_users_enhanced(list(usernames), account, "search", query)
                        account.users_scraped_today += saved_count
                        account.save()
                    self.logger.info(f"Saved final batch of {saved_count} usernames for query {query}")
                    cache_key = f"alert_save_final_batch_search_{query}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Saved final batch of {saved_count} usernames for query {query}", "info", account)
                        cache.set(cache_key, True, timeout=60)
                    time.sleep(random.uniform(5, 15))  # Delay after final batch save
                except Exception as e:
                    self.logger.error(f"Failed to save final batch for query {query}: {e}")
                    send_alert(f"Failed to save final batch for query {query}: {e}", "error", account)

            self.logger.info(
                f"Completed scraping for query {query}: "
                f"{total_users_processed} total users processed, "
                f"{account.users_scraped_today} total users saved"
            )
            cache_key = f"alert_completed_search_{query}_{account.id}"
            if not cache.get(cache_key):
                send_alert(
                    f"Completed scraping for query {query}: "
                    f"{total_users_processed} total users processed, "
                    f"{account.users_scraped_today} total users saved", "info", account
                )
                cache.set(cache_key, True, timeout=60)

        except Exception as e:
            self.logger.error(f"Search scrape failed for {query}: {e}")
            send_alert(f"Search scrape failed for {query}: {e}", "error", account)
            if usernames:
                try:
                    with transaction.atomic():
                        saved_count = self.store_users_enhanced(list(usernames), account, "search", query)
                        account.users_scraped_today += saved_count
                        account.save()
                    self.logger.info(f"Saved {saved_count} usernames before error for query {query}")
                    cache_key = f"alert_save_before_error_search_{query}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Saved {saved_count} usernames before error for query {query}", "info", account)
                        cache.set(cache_key, True, timeout=60)
                    time.sleep(random.uniform(5, 15))  # Delay after error save
                except Exception as e:
                    self.logger.error(f"Failed to save usernames before error for query {query}: {e}")
                    send_alert(f"Failed to save usernames before error for query {query}: {e}", "error", account)

        return []  # Return empty list since saving is handled internally

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
        try:
            with transaction.atomic():
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
        except Exception as e:
            self.logger.error(f"Failed to handle client error for {account.username}: {e}")
            send_alert(f"Failed to handle client error for {account.username}: {e}", "error", account)


    def _validate_hashtag(self, cl, hashtag):
        """Validate if hashtag exists and is accessible"""
        try:
            info = cl.hashtag_info(hashtag)
            return info.media_count > 0
        except Exception as e:
            self.logger.warning(f"Invalid hashtag {hashtag}: {e}")
            send_alert(f"Invalid hashtag {hashtag}: {e}", "warning")
            return False

    def store_users_enhanced(self, usernames, account, source_type, source_value):
        """Store minimal user data in bulk and queue detailed enrichment"""
        from .tasks import enrich_user_details_task
        from django.db import transaction

        saved_count = 0
        skipped_count = 0
        users_to_create = []

        try:
            with transaction.atomic():
                # Check existing users to avoid duplicates
                existing_user_ids = set(
                    ScrapedUser.objects.filter(
                        account=account,
                        user_id__in=[user_id for _, user_id in usernames]
                    ).values_list('user_id', flat=True)
                )

                for username, user_id in usernames:
                    if user_id in existing_user_ids:
                        skipped_count += 1
                        continue
                    try:
                        users_to_create.append(ScrapedUser(
                            username=username,
                            user_id=user_id,
                            account=account,
                            source_type=source_type,
                            source_value=source_value,
                            is_active=True,  # Assume active until proven otherwise
                            details_fetched=False,  # Mark as not enriched
                            is_private=False  # Initialize as not private
                        ))
                    except Exception as e:
                        self.logger.error(f"Failed to prepare user {username} (ID: {user_id}): {e}")
                        send_alert(f"Failed to prepare user {username} (ID: {user_id}): {e}", "error", account)
                        skipped_count += 1

                if users_to_create:
                    ScrapedUser.objects.bulk_create(users_to_create, ignore_conflicts=True)
                    saved_count = len(users_to_create)
                    self.logger.info(f"Stored {saved_count} users, skipped {skipped_count} for {source_value}")
                    cache_key = f"alert_store_users_{source_value}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"Stored {saved_count} users, skipped {skipped_count} for {source_value}", "info",
                                   account)
                        cache.set(cache_key, True, timeout=60)
                    # Queue enrichment task for saved users
                    enrich_user_details_task.delay()
                else:
                    self.logger.info(f"No users stored, skipped {skipped_count} for {source_value}")
                    cache_key = f"alert_store_users_{source_value}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(f"No users stored, skipped {skipped_count} for {source_value}", "info", account)
                        cache.set(cache_key, True, timeout=60)

            return saved_count
        except Exception as e:
            self.logger.error(f"Failed to store users for {source_value}: {e}")
            send_alert(f"Failed to store users for {source_value}: {e}", "error", account)
            raise

    def _get_user_detailed_info(self, cl, username):
        """Get detailed user info including accurate last post date, avoiding pinned posts"""
        try:
            user_id = cl.user_id_from_username(username)
            user_info = cl.user_info_by_username(username)

            # --- Fetch 3–6 recent posts to handle pinned ones ---
            # last_post_date = None
            # if user_info.media_count > 0:
            #     time.sleep(random.uniform(5, 10))  # delay between requests
            #     num_to_fetch = random.randint(3, 6)
            #     medias = cl.user_medias(user_id, amount=num_to_fetch)
            #
            #     # Filter valid posts with timestamps and find the most recent
            #     valid_medias = [
            #         m for m in medias if getattr(m, "taken_at", None) is not None
            #     ]
            #     if valid_medias:
            #         latest_media = max(valid_medias, key=lambda m: m.taken_at)
            #         last_post_date = latest_media.taken_at.strftime("%Y-%m-%d %H:%M:%S")

            is_active = user_info.media_count > 0
            last_post_date = None
            return {
                "user_id": str(user_id),
                "biography": getattr(user_info, "biography", "") or "",
                "follower_count": getattr(user_info, "follower_count", 0),
                "following_count": getattr(user_info, "following_count", 0),
                "post_count": getattr(user_info, "media_count", 0),
                "last_post_date": last_post_date,
                "is_active": is_active,
            }

        except PrivateError:
            self.logger.info(f"User {username} is private, skipping")
            send_alert(f"User {username} is private, skipping", "info")
            ScrapedUser.objects.filter(username=username).update(
                is_private=True, is_active=False, failure_reason="Private account"
            )
            return None
        except UserNotFound:
            self.logger.info(f"User {username} not found, skipping")
            send_alert(f"User {username} not found, skipping", "info")
            ScrapedUser.objects.filter(username=username).update(
                is_active=False, failure_reason="User not found"
            )
            return None
        except ClientError as e:
            if "401" in str(e).lower():
                self.logger.info(f"User {username} is private (401 error), skipping")
                send_alert(f"User {username} is private (401 error), skipping", "info")
                ScrapedUser.objects.filter(username=username).update(
                    is_private=True, is_active=False, failure_reason="Private account (401 error)"
                )
                return None
            self.logger.error(f"Error getting user info for {username}: {e}")
            send_alert(f"Error getting user info for {username}: {e}", "error")
            return None
        except Exception as e:
            self.logger.error(f"Error getting user info for {username}: {e}")
            send_alert(f"Error getting user info for {username}: {e}", "error")
            return None