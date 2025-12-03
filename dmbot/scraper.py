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

from .models import ScrapedUser, ProcessedMedia, DailyMetric
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
            hashtags = [
                "art", "design", "photography", "artist", "artwork", "creative", "illustration",
                "drawing", "painting", "digitalart", "graphicdesign", "artoftheday", "photooftheday",
                "fineart", "modernart", "contemporaryart", "artgallery", "artcollectors",
                "artlovers", "instaart", "instadesign", "instaphotography", "visualart",
                "aesthetic", "creativity", "inspiration", "sketch", "sketchbook", "portrait",
                "landscape", "abstract", "minimalism", "designinspiration", "interiordesign",
                "architecture", "branding", "logo", "typography", "motiondesign", "animation",
                "3dart", "3dmodeling", "digitalpainting", "conceptart", "characterdesign",
                "photographer", "photoart", "streetphotography", "travelphotography",
                "naturephotography", "blackandwhite", "bnw", "portraitphotography",
                "fashionphotography", "filmphotography", "macro", "nightphotography",
                "artcommunity", "artlife", "creativeprocess", "artistic", "mixedmedia",
                "surrealism", "expressionism", "popart", "realism", "gallery", "museum",
                "artshow", "artstudio", "artoftheworld", "designlife", "uxdesign", "uidesign",
                "productdesign", "industrialdesign", "graphicdesigner", "photoshoot", "lens",
                "dslr", "mirrorless", "lightroom", "photoshop", "capture", "composition",
                "color", "visualdesign", "artdirection", "creativedirection", "illustrator",
                "artistsoninstagram", "designers", "creatives", "handmade", "craft", "collage",
                "digitalillustration", "drawingoftheday", "artdaily", "photodaily",
                "fineartphotography", "artgram", "creativeart", "instaartist", "artoftheweek",
                "designcommunity", "colorgrading", "photoediting", "creativephotography",
                "editorialdesign", "fashiondesign", "artjournal", "paintingart", "artcollection",
                "inspired", "designstudio", "creativeindustry", "illustrationart", "designthinking",
                "artworld", "photojournalism", "visualstorytelling", "photoediting", "canon",
                "nikon", "sonyalpha", "fujifilm", "leica", "cinematography", "streetart",
                "graffiti", "urbanart", "digitalcreator", "contentcreator", "visuals", "aestheticart"
            ]
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

            # amount = self._calculate_scrape_amount(account, amount)
            amount = account.daily_scrape_limit - account.users_scraped_today

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
                    self._scrape_hashtag_likers(cl, source_id, account, source_type, amount)
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


    def _scrape_hashtag_likers(self, cl, hashtag, account, source_type="hashtag", amount=None):
        """Scrape likers, commenters, and post owners with strict limit enforcement."""
        usernames = set()
        batch_size = getattr(settings, 'SCRAPING_BATCH_SIZE', 1000)
        total_likers_processed = 0
        total_commenters_processed = 0
        total_posts_processed = 0
        max_posts = 100
        num_posts = random.randint(20, 50)
        max_likers_per_media = 50
        max_comments_per_media = 50

        # ============= EARLY LIMIT CHECK =============
        if not amount:
            amount = account.daily_scrape_limit - account.users_scraped_today

        if amount <= 0:
            self.logger.info(f"Account {account.username} has reached daily scrape limit")
            send_alert(f"Account {account.username} has reached daily scrape limit", "info", account)
            return []

        # Check global daily metric threshold
        today = timezone.now().date()
        daily_metric, _ = DailyMetric.objects.get_or_create(date=today)

        if daily_metric.scraping_limit_reached or daily_metric.scraped_count >= daily_metric.scraping_threshold:
            self.logger.warning(
                f"Global scraping threshold reached ({daily_metric.scraped_count}/{daily_metric.scraping_threshold})")
            send_alert(f"Global scraping threshold reached for {today}", "warning", account)
            with transaction.atomic():
                daily_metric.scraping_limit_reached = True
                daily_metric.save()
            return []
        # ============================================

        try:
            # Validate hashtag (keeping existing code)
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
                    return self._scrape_search(cl, hashtag, amount=amount, account=account)
            except ClientError as e:
                self.logger.error(f"Failed to fetch hashtag info for {hashtag}: {e}")
                send_alert(f"Failed to fetch hashtag info for {hashtag}: {e}", "error", account)
                return self._scrape_search(cl, hashtag, amount=amount, account=account)
            except Exception as e:
                self.logger.error(f"Unexpected error fetching hashtag info for {hashtag}: {e}")
                send_alert(f"Unexpected error fetching hashtag info for {hashtag}: {e}", "error", account)
                return self._scrape_search(cl, hashtag, amount=amount, account=account)

            # Fetch top posts (keeping existing code)
            try:
                top_medias = cl.hashtag_medias_top(hashtag, amount=num_posts)
                random.shuffle(top_medias)
                top_medias = top_medias[:max_posts]
                if not top_medias:
                    self.logger.warning(f"No media found for hashtag {hashtag}, falling back to search")
                    send_alert(f"No media found for hashtag {hashtag}, falling back to search", "warning", account)
                    return self._scrape_search(cl, hashtag, amount=amount, account=account)
                self.logger.info(f"Scraping users for hashtag {hashtag}, {len(top_medias)} posts (top)")
                cache_key = f"alert_scraping_users_{hashtag}_{account.id}"
                if not cache.get(cache_key):
                    send_alert(f"Scraping users for hashtag {hashtag}, {len(top_medias)} posts (top)", "info", account)
                    cache.set(cache_key, True, timeout=60)
            except (ValidationError, ClientError) as e:
                self.logger.error(f"Failed to fetch media for {hashtag}: {e}")
                return self._scrape_search(cl, hashtag, amount=amount, account=account)
            except Exception as e:
                self.logger.error(f"Unexpected error fetching media for {hashtag}: {e}")
                return self._scrape_search(cl, hashtag, amount=amount, account=account)

            posts_processed = 0

            for media in top_medias:
                # ============= CHECK LIMITS BEFORE EACH POST =============
                account.refresh_from_db()  # Get latest values

                remaining_capacity = account.daily_scrape_limit - account.users_scraped_today
                if remaining_capacity <= 0:
                    self.logger.info(f"Account {account.username} reached daily limit during scraping")
                    send_alert(f"Account {account.username} reached daily scrape limit ({account.daily_scrape_limit})",
                               "warning", account)
                    break

                # Check global threshold again
                daily_metric.refresh_from_db()
                if daily_metric.scraping_limit_reached or daily_metric.scraped_count >= daily_metric.scraping_threshold:
                    self.logger.warning(f"Global threshold reached during scraping")
                    send_alert(f"Global scraping threshold reached, stopping", "warning", account)
                    break
                # =========================================================

                # Check if media is already processed
                if ProcessedMedia.objects.filter(media_id=media.pk, hashtag=hashtag, account=account).exists():
                    self.logger.info(f"Skipping already processed media {media.pk} for hashtag {hashtag}")
                    continue

                try:
                    # Collect post owner
                    try:
                        if media.user and media.user.username:
                            usernames.add((media.user.username, media.user.pk))
                            self.logger.info(f"Added post owner {media.user.username} (ID: {media.user.pk})")
                    except Exception as e:
                        self.logger.warning(f"Failed to get post owner for media {media.pk}: {e}")

                    # Save batch if needed (with limit check)
                    if len(usernames) >= batch_size:
                        saved_count = self._save_batch_with_limit_check(
                            usernames, account, source_type, hashtag, amount, daily_metric
                        )
                        if saved_count == -1:  # Limit reached signal
                            return []
                        usernames.clear()
                        time.sleep(random.uniform(5, 15))

                    # Collect likers
                    try:
                        likers = cl.media_likers(media.pk)
                        media_likers_count = len(likers)
                        total_likers_processed += media_likers_count
                        self.logger.info(f"Fetched {media_likers_count} likers for media {media.pk}")

                        for liker in likers:
                            if liker.username and liker.pk:
                                usernames.add((liker.username, liker.pk))

                        # Save batch if needed (with limit check)
                        if len(usernames) >= batch_size:
                            saved_count = self._save_batch_with_limit_check(
                                usernames, account, source_type, hashtag, amount, daily_metric
                            )
                            if saved_count == -1:  # Limit reached signal
                                return []
                            usernames.clear()
                            time.sleep(random.uniform(5, 15))

                    except RateLimitError:
                        self.logger.warning(f"Rate limit hit for media {media.pk} (likers)")
                        send_alert(f"Rate limit hit for media {media.pk} on {hashtag} (likers)", "warning", account)
                        account.status = "rate_limited"
                        account.task_id = None
                        account.save()
                        time.sleep(random.uniform(60, 120))
                        continue
                    except (ClientError, ValidationError) as e:
                        self.logger.warning(f"Failed to fetch likers for media {media.pk}: {e}")
                        continue

                    # Collect commenters
                    try:
                        comments = cl.media_comments(media.pk, amount=max_comments_per_media)
                        media_commenters_count = len(comments)
                        total_commenters_processed += media_commenters_count
                        self.logger.info(f"Fetched {media_commenters_count} comments for media {media.pk}")

                        for comment in comments:
                            if comment.user.username and comment.user.pk:
                                usernames.add((comment.user.username, comment.user.pk))

                        # Save batch if needed (with limit check)
                        if len(usernames) >= batch_size:
                            saved_count = self._save_batch_with_limit_check(
                                usernames, account, source_type, hashtag, amount, daily_metric
                            )
                            if saved_count == -1:  # Limit reached signal
                                return []
                            usernames.clear()
                            time.sleep(random.uniform(5, 15))

                    except RateLimitError:
                        self.logger.warning(f"Rate limit hit for media {media.pk} (comments)")
                        send_alert(f"Rate limit hit for media {media.pk} on {hashtag} (comments)", "warning", account)
                        account.status = "rate_limited"
                        account.task_id = None
                        account.save()
                        time.sleep(random.uniform(60, 120))
                        continue
                    except (ClientError, ValidationError) as e:
                        self.logger.warning(f"Failed to fetch comments for media {media.pk}: {e}")
                        continue

                    total_likers_processed += media_likers_count
                    total_commenters_processed += media_commenters_count

                    # Save batch if needed (after all users for this post)
                    if len(usernames) >= batch_size:
                        saved_count = self._save_batch_with_limit_check(
                            usernames, account, source_type, hashtag, amount, daily_metric
                        )
                        if saved_count == -1:  # Limit reached signal
                            return []
                        usernames.clear()
                        time.sleep(random.uniform(5, 15))

                    # Mark media as processed
                    try:
                        ProcessedMedia.objects.create(
                            media_id=media.pk,
                            hashtag=hashtag,
                            account=account,
                            source_type=source_type
                        )
                        self.logger.info(f"Marked media {media.pk} as processed for hashtag {hashtag}")
                    except Exception as e:
                        self.logger.warning(f"Failed to mark media {media.pk} as processed: {e}")

                    posts_processed += 1
                    total_posts_processed += 1

                    # Human-like behavior
                    if random.random() < 0.5:
                        self._like_posts(cl, count=1)
                        time.sleep(random.uniform(30, 60))
                    if random.random() < 0.2:
                        count = random.randint(3, 7)
                        self._like_posts(cl, count=count)
                        time.sleep(random.uniform(20, 40))
                    if random.random() < 0.1:
                        count = random.randint(2, 4)
                        self._browse_hashtags(cl, count=count)
                        time.sleep(random.uniform(40, 80))
                    time.sleep(random.uniform(120, 300))

                except Exception as e:
                    self.logger.warning(f"Failed to process media {media.pk}: {e}")
                    continue

            # Save remaining usernames
            if usernames:
                self._save_batch_with_limit_check(
                    usernames, account, source_type, hashtag, amount, daily_metric
                )

            self.logger.info(
                f"Completed scraping for hashtag {hashtag}: "
                f"{account.users_scraped_today} total users saved, "
                f"{total_likers_processed} total likers processed, "
                f"{total_commenters_processed} total commenters processed, "
                f"{total_posts_processed} total posts processed"
            )

            return []

        except Exception as e:
            self.logger.error(f"Failed to scrape hashtag {hashtag}: {e}")
            if usernames:
                self._save_batch_with_limit_check(
                    usernames, account, source_type, hashtag, amount, daily_metric
                )
            return self._scrape_search(cl, hashtag, amount=amount, account=account)

    def _save_batch_with_limit_check(self, usernames, account, source_type, source_value, amount, daily_metric):
        """
        Save batch of users with strict limit enforcement.
        Returns -1 if limit reached, otherwise returns saved_count.
        """
        try:
            with transaction.atomic():
                # Refresh to get latest counts
                account.refresh_from_db()
                daily_metric.refresh_from_db()

                # Calculate how many we can actually save
                account_remaining = account.daily_scrape_limit - account.users_scraped_today
                global_remaining = daily_metric.scraping_threshold - daily_metric.scraped_count

                max_to_save = min(len(usernames), account_remaining, global_remaining)

                if max_to_save <= 0:
                    self.logger.info(f"Limit reached, cannot save more users")
                    send_alert(
                        f"Scraping limit reached. Account: {account.users_scraped_today}/{account.daily_scrape_limit}, "
                        f"Global: {daily_metric.scraped_count}/{daily_metric.scraping_threshold}",
                        "warning",
                        account
                    )
                    return -1  # Signal to stop scraping

                # Limit the batch to what we can actually save
                usernames_to_save = list(usernames)[:max_to_save]

                saved_count = self.store_users_enhanced(usernames_to_save, account, source_type, source_value)

                # Update counters
                account.users_scraped_today += saved_count
                account.save()

                daily_metric.scraped_count += saved_count
                if daily_metric.scraped_count >= daily_metric.scraping_threshold:
                    daily_metric.scraping_limit_reached = True
                daily_metric.save()

                self.logger.info(
                    f"Saved batch of {saved_count} users. "
                    f"Account: {account.users_scraped_today}/{account.daily_scrape_limit}, "
                    f"Global: {daily_metric.scraped_count}/{daily_metric.scraping_threshold}"
                )

                # Check if we hit the limit
                if account.users_scraped_today >= account.daily_scrape_limit:
                    send_alert(
                        f"Account {account.username} reached daily scrape limit ({account.daily_scrape_limit})",
                        "warning",
                        account
                    )
                    return -1

                if daily_metric.scraped_count >= daily_metric.scraping_threshold:
                    send_alert(
                        f"Global scraping threshold reached ({daily_metric.scraping_threshold})",
                        "warning",
                        account
                    )
                    return -1

                return saved_count

        except Exception as e:
            self.logger.error(f"Failed to save batch: {e}")
            send_alert(f"Failed to save batch: {e}", "error", account)
            return 0

    def _scrape_search(self, cl, query, amount, account):
        """Fallback search method with strict limit enforcement"""
        usernames = set()
        batch_size = getattr(settings, 'SCRAPING_BATCH_SIZE', 1000)
        total_users_processed = 0

        # ============= EARLY LIMIT CHECK =============
        account.refresh_from_db()
        remaining_capacity = account.daily_scrape_limit - account.users_scraped_today

        if remaining_capacity <= 0:
            self.logger.info(f"Account {account.username} has reached daily scrape limit")
            send_alert(f"Account {account.username} has reached daily scrape limit", "info", account)
            return []

        # Check global threshold
        today = timezone.now().date()
        daily_metric, _ = DailyMetric.objects.get_or_create(date=today)

        if daily_metric.scraping_limit_reached or daily_metric.scraped_count >= daily_metric.scraping_threshold:
            self.logger.warning(f"Global scraping threshold reached")
            send_alert(f"Global scraping threshold reached for {today}", "warning", account)
            return []

        # Adjust amount to respect limits
        amount = min(amount, remaining_capacity, daily_metric.scraping_threshold - daily_metric.scraped_count)
        # ============================================

        try:
            self.logger.info(f"Scraping users for search query {query}, targeting {amount} users")
            send_alert(f"Scraping users for search query {query}, targeting {amount} users", "info", account)

            results = cl.search_users(query)
            for user in results:
                # ============= CHECK LIMIT DURING LOOP =============
                account.refresh_from_db()
                if account.users_scraped_today >= account.daily_scrape_limit:
                    self.logger.info(f"Account limit reached during search")
                    send_alert(f"Account {account.username} reached daily limit during search", "warning", account)
                    break

                daily_metric.refresh_from_db()
                if daily_metric.scraped_count >= daily_metric.scraping_threshold:
                    self.logger.info(f"Global limit reached during search")
                    send_alert(f"Global threshold reached during search", "warning", account)
                    break
                # ==================================================

                try:
                    if user.username and user.pk:
                        usernames.add((user.username, user.pk))
                        total_users_processed += 1

                        # Save usernames in batches
                        if len(usernames) >= batch_size:
                            saved_count = self._save_batch_with_limit_check(
                                usernames, account, "search", query, amount, daily_metric
                            )
                            if saved_count == -1:  # Limit reached
                                return []
                            usernames.clear()
                            time.sleep(random.uniform(5, 15))

                    if total_users_processed >= amount:
                        break

                except Exception as e:
                    self.logger.warning(f"Failed to process user for query {query}: {e}")
                    continue

            # Save any remaining usernames
            if usernames:
                self._save_batch_with_limit_check(
                    usernames, account, "search", query, amount, daily_metric
                )

            self.logger.info(
                f"Completed scraping for query {query}: "
                f"{total_users_processed} total users processed, "
                f"{account.users_scraped_today} total users saved"
            )

        except Exception as e:
            self.logger.error(f"Search scrape failed for {query}: {e}")
            if usernames:
                self._save_batch_with_limit_check(
                    usernames, account, "search", query, amount, daily_metric
                )

        return []

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
        """
        Store minimal user data in bulk with strict limit enforcement.
        Returns actual number of users saved (may be less than requested).
        """
        from .tasks import enrich_user_details_task
        from django.db import transaction
        from django.utils import timezone

        saved_count = 0
        skipped_count = 0
        users_to_create = []

        try:
            with transaction.atomic():
                # ============= LIMIT CHECK BEFORE SAVING =============
                account.refresh_from_db()

                # Check account limit
                remaining_capacity = account.daily_scrape_limit - account.users_scraped_today
                if remaining_capacity <= 0:
                    self.logger.info(
                        f"Account {account.username} has reached daily scrape limit, cannot store more users"
                    )
                    send_alert(
                        f"Account {account.username} reached daily scrape limit ({account.daily_scrape_limit})",
                        "warning",
                        account
                    )
                    return 0

                # Check global limit
                today = timezone.now().date()
                daily_metric, _ = DailyMetric.objects.get_or_create(date=today)

                global_remaining = daily_metric.scraping_threshold - daily_metric.scraped_count
                if global_remaining <= 0 or daily_metric.scraping_limit_reached:
                    self.logger.warning(
                        f"Global scraping threshold reached, cannot store more users"
                    )
                    send_alert(
                        f"Global scraping threshold reached ({daily_metric.scraping_threshold})",
                        "warning",
                        account
                    )
                    return 0

                # Calculate actual number we can save
                max_can_save = min(len(usernames), remaining_capacity, global_remaining)

                if max_can_save <= 0:
                    self.logger.info(f"Cannot save any users due to limits")
                    return 0

                # Limit the batch to what we can actually save
                usernames = usernames[:max_can_save]
                # ====================================================

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
                            is_active=True,
                            details_fetched=False,
                            is_private=False
                        ))
                    except Exception as e:
                        self.logger.error(f"Failed to prepare user {username} (ID: {user_id}): {e}")
                        send_alert(f"Failed to prepare user {username} (ID: {user_id}): {e}", "error", account)
                        skipped_count += 1

                if users_to_create:
                    ScrapedUser.objects.bulk_create(users_to_create, ignore_conflicts=True)
                    saved_count = len(users_to_create)

                    # ============= UPDATE COUNTERS =============
                    # Note: The calling function should also update these,
                    # but we do it here for safety
                    account.users_scraped_today += saved_count
                    account.save()

                    daily_metric.scraped_count += saved_count
                    if daily_metric.scraped_count >= daily_metric.scraping_threshold:
                        daily_metric.scraping_limit_reached = True
                    daily_metric.save()
                    # ==========================================

                    self.logger.info(
                        f"Stored {saved_count} users, skipped {skipped_count} for {source_value}. "
                        f"Account: {account.users_scraped_today}/{account.daily_scrape_limit}, "
                        f"Global: {daily_metric.scraped_count}/{daily_metric.scraping_threshold}"
                    )
                    cache_key = f"alert_store_users_{source_value}_{account.id}"
                    if not cache.get(cache_key):
                        send_alert(
                            f"Stored {saved_count} users, skipped {skipped_count} for {source_value}",
                            "info",
                            account
                        )
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