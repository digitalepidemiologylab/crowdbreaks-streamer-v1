from app.connections.elastic import Elastic
from app.settings import Config
from app.utils.redis import Redis
from app.utils.priority_queue import PriorityQueue
from app.utils.process_tweet import ProcessTweet
import logging
import re
import en_core_web_sm
from datetime import datetime
import pandas as pd
import numpy as np


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
        self.redis = Redis()
        if not isinstance(project_keywords, list):
            self.project_keywords = []
        else:
            self.project_keywords = project_keywords
        # Counts queue: holds mixture of retweet + tweet token counts
        self.pq_counts_weighted = PriorityQueue(project,
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
        # set blacklisted tokens (to be ignored by tokenizer)
        self.default_blacklisted_tokens = ['RT', 'breaking', 'amp', 'covid19', 'covid-19', 'coronaviru']
        self.blacklisted_tokens = self._generate_blacklist_tokens(project_keywords=project_keywords)

    def get_trending_topics(self, num_topics, method='ms', length=300, alpha=.5, field='counts', use_cache=True):
        trends = self.get_trends(length=length, alpha=alpha, field=field, use_cache=use_cache)
        if len(trends) == 0:
            return []
        df = pd.DataFrame.from_dict(trends, orient='index')
        df.sort_values(method, inplace=True, ascending=False)
        items = df.iloc[:num_topics][method].index.tolist()
        return items

    def get_trending_topics_df(self, length=300, alpha=.5, field='counts', use_cache=True):
        trends = self.get_trends(length=length, alpha=alpha, field=field, use_cache=use_cache)
        df = pd.DataFrame.from_dict(trends, orient='index')
        return df

    def get_trends(self, length=300, alpha=.5, field='counts', use_cache=True):
        current_hour = datetime.utcnow().strftime('%Y-%m-%d-%H')
        cache_key = f'cb:cached-trending-topics-velocities-{current_hour}-{length}-{alpha}-{field}'
        if self.redis.exists(cache_key) and use_cache:
            return self.redis.get_cached(cache_key)
        # retrieve all data from ES
        df = self.es.get_trending_topics(self.trending_topics_index_name, field=field, s_date='now-1d', interval='hour', with_moving_average=True)
        if len(df) == 0:
            return {}
        # compute velocities for a few different methods
        df = pd.DataFrame.from_records(df)
        # pivot table and make sure we only have one value per bucket (take the mean if there are multiple)
        df_counts = df.pivot(index='bucket_time', columns='term', values='value').resample('H').mean()
        df_ma = df.pivot(index='bucket_time', columns='term', values='moving_average').resample('H').mean()
        # fill all nans with zero
        df_counts = df_counts.fillna(0)
        df_ma = df_ma.fillna(0)
        trends = {}
        for term in df_counts:
            velocity = {}
            counts = df_counts[term]
            # make sure index is sorted
            counts.sort_index(inplace=True)
            # ms
            current_value = counts.iloc[-1]
            last_hour = counts.iloc[-2]
            at_24h = counts.iloc[0]
            v_1h = (current_value - last_hour)/current_value**alpha
            v_24h = (current_value - at_24h)/current_value**alpha
            velocity['ms'] = v_1h + v_24h
            # z-scores
            zscore = (current_value - counts.mean())/counts.std()
            velocity['zscore'] = zscore
            # v_1h
            velocity['v1h'] = (current_value - last_hour)/current_value
            velocity['v1h_alpha'] = (current_value - last_hour)/current_value**alpha
            # moving average slope
            ma = df_ma[term]
            ma.sort_index(inplace=True)
            y = ma.values
            x = (ma.index - ma.index[0]).total_seconds().values
            fit = np.polyfit(x, y, 1)
            velocity['polyfit_1'] = fit[-1]
            fit = np.polyfit(x, y, 2)
            velocity['polyfit_2'] = fit[-1]
            trends[term] = velocity
        # set cache
        self.redis.set_cached(cache_key, trends, expire_in_min=60)
        return trends

    def process(self, tweet, retweet_count_increment=0.8):
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
        self.add_to_queue(self.pq_counts_weighted, tokens, count_increment)
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
        # add named entities
        allowed_entities = [
                'PERSON',        # People, including fictional.
                'NORP',          # Nationalities or religious or political groups.
                'FAC',           # Buildings, airports, highways, bridges, etc.
                'ORG',           # Companies, agencies, institutions, etc.
                'GPE',           # Countries, cities, states.
                'LOC',           # Non-GPE locations, mountain ranges, bodies of water.
                'PRODUCT',       # Objects, vehicles, foods, etc. (Not services.)
                'EVENT',         # Named hurricanes, battles, wars, sports events, etc.
                'WORK_OF_ART',   # Titles of books, songs, etc.
                'LAW'            # Named documents made into laws.
                ]
        entities = [ent for ent in doc.ents if ent.label_ in allowed_entities]
        entities = list(set(entities))
        # add all entities to tokens
        tokens = entities
        # remove entities from doc
        for t in doc:
            # add all nouns longer than 2 characters
            # print(t, t.tag_)
            if t.pos_ not in ['NOUN', 'PROPN'] or len(t) <= 2:
                continue
            # make sure token was not already part of entities
            for ent in entities:
                if str(t.lemma_) in ent.lemma_:
                    break
            else:
                tokens.append(t)
        # remove all tokens which are officially blacklisted
        tokens = [t for t in tokens if t.text.lower().strip() not in self.blacklisted_tokens]
        tokens = [t for t in tokens if t.lemma_.lower().strip() not in self.blacklisted_tokens]
        # fetch lemmas
        tokens = [t.lemma_ for t in tokens]
        return tokens

    def update(self):
        """Main function called by celery beat in a regular time interval"""
        if self.config.ENV in ['stg', 'prd']:
            self.index_counts_to_elasticsearch()
        else:
            logging.info('Indexing of trending topic counts is only run in stg/prd environments')
        # Compute trending topics so they are cached
        trends = self.get_trends(length=200, alpha=.5, field='counts')
        # clear all other counts
        self.pq_counts_weighted.self_remove()
        self.pq_counts_retweets.self_remove()
        self.pq_counts_tweets.self_remove()

    def index_counts_to_elasticsearch(self, top_n=300):
        # create index if it doesn't exist yet
        if self.trending_topics_index_name not in self.es.list_indices():
            self.es.create_index(self.trending_topics_index_name)
        # compile all count data
        data = []
        utc_now = datetime.utcnow()
        for rank, (key, counts) in enumerate(self.pq_counts_weighted.multi_pop(top_n, with_scores=True)):
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
                'rank_weighted': rank,
                'rank_tweets': rank_tweets,
                'rank_retweets': rank_retweets,
                'counts_weighted': counts,
                'counts_tweets': counts_tweets,
                'counts_retweets': counts_retweets,
                'counts': counts_total})
        # compile actions
        actions = [{'_source': d, '_index': self.trending_topics_index_name, '_type': '_doc'} for d in data]
        # bulk index
        logger.info(f'Bulk indexing of {len(actions):,} documents to index {self.trending_topics_index_name}...')
        self.es.bulk_index(actions)

    def self_remove(self):
        self.pq_counts_weighted.self_remove()
        self.pq_counts_retweets.self_remove()
        self.pq_counts_tweets.self_remove()

    # private

    def _generate_blacklist_tokens(self, project_keywords=None):
        bl_tokens = self.default_blacklisted_tokens
        if project_keywords is not None:
            bl_tokens += project_keywords
            # add hashtag versions
            bl_tokens += ['#' + t for t in bl_tokens]
            # lower case everything
            bl_tokens = [t.lower() for t in bl_tokens]
        return bl_tokens

