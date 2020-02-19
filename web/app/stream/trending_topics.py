from app.connections.elastic import Elastic
from app.settings import Config
from app.utils.redis import Redis
from app.utils.priority_queue import PriorityQueue
from app.utils.process_tweet import ProcessTweet
import logging
import re
import en_core_web_sm
from datetime import datetime


logger = logging.getLogger(__name__)
nlp = en_core_web_sm.load()

class TrendingTopics(Redis):
    """
    Compiles a priority queue of recent popular tokens

    For this we maintain a priority queue where keys are tokens and values are the number of retweets

    All tweets get processed by the process method.
    """
    def __init__(self,
            project,
            project_locales=None,
            key_namespace_counts='trending-topics-counts',
            key_namespace_counts_old='trending-topics-counts-old',
            key_namespace_velocity='trending-topics-velocity',
            max_queue_length=1e4,
            project_keywords=None
            ):
        super().__init__(self)
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.project = project
        self.max_queue_length = int(max_queue_length)
        self.project_locales = project_locales
        self.trending_topics_index_name = f'trending_topics_{project}'
        self.es = Elastic()
        if not isinstance(project_keywords, list):
            self.project_keywords = []
        else:
            self.project_keywords = project_keywords
        # Counts queue: holds mixture of retweet + tweet token counts
        self.pq_counts = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=key_namespace_counts,
                max_queue_length=self.max_queue_length)
        # Retweet counts queue: holds counts only from retweets
        self.pq_counts_retweets = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=key_namespace_counts + '-retweets',
                max_queue_length=self.max_queue_length)
        # Tweet counts queue: holds counts only by tweets
        self.pq_counts_tweets = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=key_namespace_counts + '-tweets',
                max_queue_length=self.max_queue_length)
        # Old counts queue: holds past counts (to be removed)
        self.pq_counts_old = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=key_namespace_counts_old,
                max_queue_length=self.max_queue_length)
        # Velocity queue: Computed velocities of tokens (updated at regular time intervals)
        self.pq_velocity = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=key_namespace_velocity,
                max_queue_length=self.max_queue_length)
        # set blacklisted tokens (to be ignored by tokenizer)
        self.blacklisted_tokens = self._generate_blacklist_tokens(project_keywords=project_keywords)

    def get_trending_topics(self, num_topics):
        items = self.pq_velocity.multi_pop(num_topics)
        return items

    def process(self, tweet, retweet_count_increment=0.2):
        if not self.should_be_processed(tweet):
            return
        # get tokens
        pt = ProcessTweet(project=self.project, tweet=tweet, project_locales=self.project_locales)
        tokens = self.tokenize_tweet(tweet, pt)
        # determine count increment
        count_increment = 1
        if pt.is_retweet:
            count_increment = retweet_count_increment
        # add tokens to queues
        self.add_to_queue(self.pq_counts, tokens, count_increment)
        if pt.is_retweet:
            self.add_to_queue(self.pq_counts_retweets, tokens, 1)
        else:
            self.add_to_queue(self.pq_counts_tweets, tokens, 1)

    def add_to_queue(self, queue, tokens, increment):
        for token in tokens:
            # add count increment to count queue
            if queue.exists(token):
                queue.increment_priority(token, incr=increment)
            else:
                queue.add(token, priority=increment)

    def should_be_processed(self, tweet):
        if self.project_locales is not None:
            if len(self.project_locales) > 0:
                if not tweet['lang'] in self.project_locales:
                    return False
        if 'possibly_sensitive' in tweet:
            if tweet['possibly_sensitive']:
                return False
        return True

    def tokenize_tweet(self, tweet, pt):
        text = pt.get_text()
        # remove mentions and urls
        text = pt.replace_user_mentions(text, filler='')
        text = pt.replace_urls(text, filler='')
        tokens = self.tokenize(text)
        return tokens

    def tokenize(self, text):
        # create doc
        doc = nlp(text, disable=['parser'])
        # find hashtag indices and merge again (so the # are not lost)
        hashtag_pos = []
        for i, t in enumerate(doc[:-1]):
            if t.text == '#':
                hashtag_pos.append(i)
        with doc.retokenize() as retokenizer:
            for i in hashtag_pos:
                try:
                    retokenizer.merge(doc[i:(i+2)])
                except ValueError:
                    pass
        # add all nouns longer than 2 characters
        tokens = [t.text for t in doc if t.pos_ in ['NOUN'] and len(t) > 2]
        # add named entities
        allowed_entities = [
                'PERSON',        # People, including fictional.
                'NORP',          # Nationalities or religious or political groups.
                'FAC',           # Buildings, airports, highways, bridges, etc.
                'ORG',           # Companies, agencies, institutions, etc.
                'ORG',           # Companies, agencies, institutions, etc.
                'LOC',           # Non-GPE locations, mountain ranges, bodies of water.
                'PRODUCT',       # Objects, vehicles, foods, etc. (Not services.)
                'EVENT',         # Named hurricanes, battles, wars, sports events, etc.
                'WORK_OF_ART',   # Titles of books, songs, etc.
                'LAW'            # Named documents made into laws.
                ]
        entities = [ent.text for ent in doc.ents if ent.label_ in allowed_entities]
        entities = list(set(entities))
        tokens += [ent for ent in entities if ent not in tokens]
        # remove empty (whitespace) tokens
        tokens = [t for t in tokens if len(t.strip()) > 0]
        # remove blacklisted tokens
        tokens = [t for t in tokens if t.lower() not in self.blacklisted_tokens]
        return tokens

    def update(self):
        """Main function called by celery beat in a regular time interval"""
        self.index_counts_to_elasticsearch()
        self.compute_velocity()

    def compute_velocity(self, alpha=.5, top_n=200):
        if len(self.pq_counts_old) > 0:
            self.pq_velocity.self_remove()
            items = self.pq_counts.multi_pop(top_n)
            for i, key in enumerate(items):
                current_val = self.pq_counts.get_score(key)
                old_val = 0
                if self.pq_counts_old.exists(key):
                    old_val = self.pq_counts_old.get_score(key)
                # compute velocity of trend
                velocity = (current_val-old_val)/current_val**alpha
                self.pq_velocity.add(key, velocity)
                if i > top_n:
                    break
        # First time we are running this we simply copy over the current counts to the old counts
        self._r.rename(self.pq_counts.key, self.pq_counts_old.key)
        # Todo: Instead of just replacing current with old: Calculate moving average

    def index_counts_to_elasticsearch(self, top_n=300):
        # create index if it doesn't exist yet
        if self.trending_topics_index_name not in self.es.list_indices():
            self.es.create_index(self.trending_topics_index_name)
        # compile all count data
        data = []
        utc_now = datetime.utcnow()
        for rank, (key, counts) in enumerate(self.pq_counts.multi_pop(top_n, with_scores=True)):
            # rank/counts of retweets
            counts_retweets =  self.pq_counts_retweets.get_score(key)
            if counts_retweets is None:
                counts_retweets = 0
            rank_retweets = self.pq_counts_retweets.get_rank(key)
            if rank_retweets is None:
                rank_retweets = -1
            # rank/counts of tweets
            counts_tweets =  self.pq_counts_tweets.get_score(key)
            if counts_tweets is None:
                counts_tweets = 0
            rank_tweets = self.pq_counts_tweets.get_rank(key)
            if rank_tweets is None:
                rank_tweets = -1
            # total
            counts_total = counts_tweets + counts_retweets
            data.append({
                'created_at': utc_now,
                'bucket_time': utc_now.replace(microsecond=0, second=0, minute=0),
                'hour': utc_now.hour,
                'term': key,
                'rank': rank,
                'rank_tweets': rank_tweets,
                'rank_retweets': rank_retweets,
                'counts': counts,
                'counts_tweets': counts_tweets,
                'counts_retweets': counts_retweets,
                'counts_total': counts_total})
        # compile actions
        actions = [{'_source': d, '_index': self.trending_topics_index_name, '_type': '_doc'} for d in data]
        # bulk index
        logger.info(f'Bulk indexing of {len(actions):,} documents to index {self.trending_topics_index_name}...')
        self.es.bulk_index(actions)

    def self_remove(self):
        self.pq_counts.self_remove()
        self.pq_counts_retweets.self_remove()
        self.pq_counts_tweets.self_remove()
        self.pq_counts_old.self_remove()
        self.pq_velocity.self_remove()

    # private

    def _generate_blacklist_tokens(self, project_keywords=None):
        bl_tokens = ['RT']
        if project_keywords is not None:
            bl_tokens += project_keywords
            # add hashtag versions
            bl_tokens += ['#' + t for t in bl_tokens]
            # lower case everything
            bl_tokens = [t.lower() for t in bl_tokens]
        return bl_tokens

