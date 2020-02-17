from app.settings import Config
from app.utils.redis import Redis
from app.utils.priority_queue import PriorityQueue
from app.utils.process_tweet import ProcessTweet
import logging
import re
import en_core_web_sm

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
            key_namespace='trending-topics',
            max_queue_length=1e4,
            project_keywords=None
            ):
        super().__init__(self)
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.project = project
        self.key_namespace = key_namespace
        self.max_queue_length = int(max_queue_length)
        self.project_locales = project_locales
        if not isinstance(project_keywords, list):
            self.project_keywords = []
        else:
            self.project_keywords = project_keywords
        self.pq = PriorityQueue(project,
                namespace=self.namespace,
                key_namespace=self.key_namespace,
                max_queue_length=self.max_queue_length)
        # set blacklisted tokens (ignored by tokenizer)
        self.blacklisted_tokens = ['RT'] + self.project_keywords
        self.blacklisted_tokens = [t.lower() for t in self.blacklisted_tokens]

    def get_trending_topics(self, num_topics, min_score=100):
        items = self.pq.multi_pop(num_topics, min_score=min_score)
        return items

    def process(self, tweet):
        if not self.should_be_processed(tweet):
            return
        tokens = self.tokenize_tweet(tweet)
        for token in tokens:
            if self.pq.exists(token):
                self.pq.increment_priority(token, incr=1)
            else:
                self.pq.add(token, priority=1)

    def should_be_processed(self, tweet):
        if self.project_locales is not None:
            if len(self.project_locales) > 0:
                if not tweet['lang'] in self.project_locales:
                    return False
        if 'possibly_sensitive' in tweet:
            if tweet['possibly_sensitive']:
                return False
        return True

    def tokenize_tweet(self, tweet):
        pt = ProcessTweet(project=self.project, tweet=tweet, project_locales=self.project_locales)
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
                retokenizer.merge(doc[i:(i+2)])
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

    def forget_topics(self, top_counts=1000):
        # get highest valued item
        key = self.pq.pop()
        highest_score = self.pq.get_score(key)
        if highest_score < top_counts:
            logger.info(f'Max topic has less than {top_counts:,} counts. Aborting.')
            return
        # calculate normalization factor for top candidate
        forgetting_factor = top_counts/highest_score
        logger.info(f'Shrinking all topic counts by a factor {forgetting_factor:.5f}')
        for key, val in self.pq:
            incr = forgetting_factor*val - val
            self.pq.increment_priority(key, incr=incr)

    def self_remove(self):
        self.pq.self_remove()
        assert len(self.pq) == 0
