from app.settings import Config
from app.utils.redis import Redis
from app.connections.elastic import Elastic
from app.utils.priority_queue import PriorityQueue
import logging

logger = logging.getLogger(__name__)


class TrendingTweets(Redis):
    """
    Compiles a priority queue of recent popular (most retweeted) tweets.

    For this we maintain two data structures:
    1) A Redis based priority queue: keys are tweet ids and integers are the number of retweets
    2) Eeach tweet id has a key which expires after a certain time. A cleanup crontab will then delete all keys from the priority queue which have been expired (see cleanup method).

    All tweets get processed by the process method.
    """
    def __init__(self,
            project,
            project_config=None,
            es_index_name=None,
            project_locales=None,
            key_namespace='trending-tweets',
            max_queue_length=1e4,
            expiry_time_ms=2*24*3600*1000):
        super().__init__(self)
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.project = project
        self.key_namespace = key_namespace
        self.max_queue_length = int(max_queue_length)
        self.pq = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=self.key_namespace,
                max_queue_length=self.max_queue_length)
        self.expiry_time_ms = expiry_time_ms
        self.es_index_name = es_index_name
        self.es = Elastic()
        self.project_locales = project_locales

    def expiry_key(self, tweet_id):
        return "{}:{}:{}:{}:{}".format(self.namespace, self.key_namespace, self.project, 'expiry-key', tweet_id)

    def get_trending_tweets(self, num_tweets, query='', sample_from=0, min_score=0):
        if query == '' or self.es_index_name is None:
            items = self.pq.multi_pop(num_tweets, sample_from=sample_from, min_score=min_score)
        else:
            # Get large enough sample of IDs to search in
            items = self.pq.multi_pop(self.max_queue_length, min_score=min_score)
            # Match retrieved documents for specific query
            items = self.es.get_matching_ids_for_query(self.es_index_name, query, items, size=num_tweets)
        return items

    def process(self, tweet):
        if not self.should_be_processed(tweet):
            return
        retweeted_id = tweet['retweeted_status']['id_str']
        if self.pq.exists(retweeted_id):
            self.pq.increment_priority(retweeted_id, incr=1)
        else:
            self.pq.add(retweeted_id, priority=1)
            # set an expiry key
            self._r.psetex(self.expiry_key(retweeted_id), self.expiry_time_ms, 1)

    def should_be_processed(self, tweet):
        if not 'retweeted_status' in tweet:
            return False
        if self.project_locales is not None:
            if len(self.project_locales) > 0:
                if not tweet['lang'] in self.project_config['locales']:
                    return False
        if 'possibly_sensitive' in tweet:
            if tweet['possibly_sensitive']:
                return False
        if 'possibly_sensitive' in tweet['retweeted_status']:
            if tweet['possibly_sensitive']:
                return False
        return True

    def cleanup(self):
        num_deleted = 0
        for key, _ in self.pq:
            key_dec = key.decode()
            if not self._r.exists(self.expiry_key(key_dec)):
                self.pq.remove(key_dec)
                num_deleted +=  1
        logger.info(f'Deleted {num_deleted:,} expired keys from priority queue')

    def self_remove(self):
        self.pq.self_remove()
        assert len(self.pq) == 0
        for k in self._r.scan_iter(self.expiry_key('*')):
            self._r.delete(k)
