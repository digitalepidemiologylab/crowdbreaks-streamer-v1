import redis
import os
import random
import logging
from app.utils.redis import Redis
from helpers import report_error
import json
import collections
import numpy as np


class PriorityQueue(Redis):
    """For each project keep a priority queue of tweet IDs in Redis to quickly get a new tweet to classify"""

    MAX_ELEMENT_PRINT = 100  # maximum number of items to print when printing an instance of this class
    MAX_QUEUE_LENGTH = 1000

    def __init__(self, project, namespace='cb', key_namespace='pq', max_queue_length=1000, **args):
        super().__init__(self)
        # logging
        self.logger = logging.getLogger('PriorityQueue')
        self.project = project
        self.namespace = namespace
        self.key_namespace = key_namespace
        self.MAX_QUEUE_LENGTH = max_queue_length

    def __len__(self):
        return self._r.zcard(self.key)

    def __repr__(self):
        output = "{}<key={}>\n".format(self.__class__.__name__, self.key)
        output += "#   Priority  Value\n"
        count = 1
        for item in self:
            output += "{:02d}) {:0.1f}       {}\n".format(count, item[1], item[0].decode())
            count += 1
            if count > self.MAX_ELEMENT_PRINT:
                break
        return output[:-1]

    def __iter__(self):
        return self._r.zscan_iter(self.key)

    def __bool__(self):
        return self._r.exists(self.key) > 0

    @property
    def key(self):
        return "{}:{}:{}".format(self.namespace, self.key_namespace, self.project)

    def add(self, value, priority=0):
        """Push value with given priority to queue. Enforce max length of queue by removing low-priority elements."""
        removed = []
        while len(self) >= self.MAX_QUEUE_LENGTH:
            item = self.remove_lowest_priority()
            if item is not None:
                removed.extend(item)
        self._r.zadd(self.key, {value: priority})
        return removed

    def pop(self, remove=False):
        """Get key with highest priority, optionally also remove that key from queue"""
        try:
            item = self._r.zrevrange(self.key, 0, 0)[0]
        except IndexError:
            # Queue is empty
            return None
        if remove:
            self.remove(item)
        return item.decode()

    def multi_pop(self, num, sample_from=0, min_score=0, remove=False, with_scores=False):
        """
        Return multiple elements.
        If sample_from > 0, compile a priority-weighted sample of `num` elements from the top `sample_from`
        """
        num_items = max(num, sample_from)
        try:
            items = self._r.zrevrangebyscore(self.key, '+Inf', min_score, start=0, num=num_items, withscores=True)
        except IndexError:
            # Queue is empty
            return None
        if sample_from > 0 and len(items) > num and len(items) > 0:
            # subsample
            keys, values = list(zip(*items))
            values = np.array(values)
            sum_values = np.sum(values)
            if sum_values > 0:
                probabilities = values / sum_values
                index = np.arange(len(items))
                index = np.random.choice(index, num, p=probabilities, replace=False)
                keys = [keys[i] for i in index]
                values = [values[i] for i in index]
        elif len(items) > 0:
            keys, values = list(zip(*items))
        else:
            keys = []
            values = []
        if remove:
            for key in keys:
                self.remove(key)
        # decode
        keys = [k.decode() for k in keys]
        values = list(values)
        if with_scores:
            return tuple(zip(keys, values))
        return keys

    def get_rank(self, val):
        return self._r.zrevrank(self.key, val)

    def increment_priority(self, val, incr=1):
        if self._r.zrank(self.key, val) is None:
            self.logger.debug("Priority of value {} cannot be changed, because it doesn't exist anymore".format(val))
        self._r.zincrby(self.key, incr, val)

    def remove_lowest_priority(self, random_deletion=True):
        """Remove key with the lowest priority"""
        if not random_deletion:
            # Just delete lowest priority key
            items = self._r.zrevrange(self.key, 0, 0, withscores=True)
            num_deleted = self._r.zremrangebyrank(self.key, 0, 0)
            if num_deleted == 0:
                report_error(self.logger, msg='Tried to remove lowest ranking element but queue is empty.', level='warning')
            return items
        # Remove a random lowest priority key
        items = self._r.zrevrange(self.key, 0, 0, withscores=True)
        if len(items) == 0:
            report_error(self.logger, msg='Tried to remove lowest ranking element but queue is empty.', level='warning')
            return
        lowest_score = items[0][1]
        num_elements = self._r.zcount(self.key, lowest_score, lowest_score)
        if num_elements == 0:
            msg = 'Element with score {} could not be found. Possibly it has been removed before. Aborting.'.format(lowest_score)
            report_error(self.logger, msg=msg, level='warning')
            return
        elif num_elements == 1:
            items = self._r.zrevrange(self.key, 0, 0, withscores=True)
            self._r.zremrangebyrank(self.key, 0, 0)
        else:
            # multiple elements with the same lowest score, randomly remove one
            rand_index = random.randint(0, num_elements-1)
            self.logger.debug('Picked {} as randindex between {} and {}'.format(rand_index, 0, num_elements-1))
            items = self._r.zrange(self.key, rand_index, rand_index, withscores=True)
            num_deleted = self._r.zremrangebyrank(self.key, rand_index, rand_index)
            if num_deleted != 1:
                report_error(self.logger, msg='Random key could not be deleted because it does not exist anymore')
        return items

    def list(self, length=100):
        """Lists priority queue as HTML"""
        pq_list = {}
        for count, item in enumerate(self):
            pq_list[item[0].decode()] = item[1]
        pq_list = sorted(pq_list.items(), key=lambda kv: kv[1], reverse=True)
        output = "<h1>{}: {}</h1>".format(self.__class__.__name__, self.project)
        output += "<p>List size: {} (max {} listed below)</p>".format(len(pq_list), length)
        output += "<p>Max list size: {}</p>".format(self.MAX_QUEUE_LENGTH)
        output += '<table align="left" border="1">'
        output += "<thead><tr><th>#</th><th>Item</th><th>Priority</th></tr></thead>"
        output += "<tbody>"
        for i, item in enumerate(pq_list):
            output += "<tr><td>{})</td><td>{}</td><td>{:.1f}</td></tr>".format(i + 1, item[0], item[1])
            if i > length:
                break
        output += "</table>"
        return output

    def remove(self, item):
        """Remove key by keyname"""
        if self._r.zrem(self.key, item) == 0:
            report_error(self.logger, msg='Element {} could not be deleted'.format(item.decode()))

    def get_score(self, val):
        return self._r.zscore(self.key, val)

    def self_remove(self):
        self._r.delete(self.key)

    def exists(self, val):
        if self._r.zrank(self.key, val) is None:
            return False
        else:
            return True

class TweetStore(Redis):
    """Stores tweets with the tweet ID as the key and the tweet as a hash"""

    def __init__(self, namespace='cb', key_namespace='tweet_store', **kwargs):
        super().__init__(self)
        self.namespace = namespace
        self.key_namespace = key_namespace

    def __repr__(self):
        s = ''
        for i, k in enumerate(self._r.scan_iter(self.key('*'))):
            tid = k.decode().split(':')[-1]
            s += '{:02d}) {}\n'.format(i+1, tid)
        return s

    def __len__(self):
        return len(self._r.keys(self.key('*')))

    def key(self, tweet_id):
        return "{}:{}:{}".format(self.namespace, self.key_namespace, tweet_id)

    def add(self, tweet):
        self._r.set(self.key(tweet['id']), json.dumps(tweet).encode())

    def get(self, tweet_id):
        tweet = self._r.get(self.key(tweet_id))
        if tweet is None:
            return
        return json.loads(tweet.decode())

    def remove(self, tweet_id):
        self._r.delete(self.key(tweet_id))

    def remove_all(self):
        for k in self._r.scan_iter(self.key('*')):
            self._r.delete(k)

class TweetIdQueue:
    """Handles Tweet IDs in a priority queue and keeps a record of which user classified what tweet as a set in Redis."""

    def __init__(self, project, namespace='cb', logger=None, priority_threshold=3, **kwargs):
        """
        :param project: Unique project name (used to name queue)
        :param namespace: Redis key namespace
        :param logger: Logger instance
        :param priority_threshold: Number of times tweet should be labelled before being removed from queue
        """
        # logging
        if logger is None:
            self.logger = logging.getLogger('PriorityQueue')
        else:
            self.logger = logger

        self.project = project
        self.pq = PriorityQueue(project, namespace=namespace, max_queue_length=kwargs.get('max_queue_length', 1000))
        self.rset = RedisSet(project, namespace=namespace, **kwargs)
        self.tweet_store = TweetStore(namespace=namespace, **kwargs)
        self.priority_threshold = priority_threshold

    def add(self, tweet_id, priority=0):
        """Simply adds a new tweet_id to its priority queue"""
        self.pq.add(tweet_id, priority=priority)

    def add_tweet(self, tweet, priority=0):
        """Adds a new tweet to its priority queue and stores it in the TweetStore"""
        removed_items = self.pq.add(tweet['id'], priority=priority)
        for item in removed_items:
            self.tweet_store.remove(item[0].decode())
        self.tweet_store.add(tweet)

    def get(self, user_id=None):
        """Get new tweet ID to classify for user ID """
        # If no user is defined, simply pop the queue
        if user_id is None:
            tweet_id = self.pq.pop()
            if tweet_id is None:
                report_error(self.logger, msg='Queue is empty')
                return None
            else:
                return tweet_id
        else:
            tweet_id = self.retrieve_for_user(user_id)
            if tweet_id is None:
                report_error(self.logger, msg='No new tweet could be found for user_id {}'.format(user_id))
            else:
                return tweet_id

    def get_tweet(self, user_id=None):
        """Get tweet to classify for user ID """
        # If no user is defined, simply pop the queue
        tweet_id = self.get(user_id=user_id)
        if tweet_id is None:
            return None
        else:
            tweet = self.tweet_store.get(tweet_id)
            if tweet is None:
                return {'id': tweet_id}
            else:
                return tweet

    def retrieve_for_user(self, user_id):
        num = 3
        starts = [num*i for i in range(1 + (len(self.pq)//num))]
        for s in starts:
            item_range = self.pq._r.zrevrangebyscore(self.pq.key, '+inf', '-inf', start=s, num=num, withscores=True)
            for tweet_id, score in item_range:
                if not self.rset.is_member(tweet_id.decode(), user_id):
                    return tweet_id.decode()

    def update(self, tweet_id, user_id):
        """Track the fact that user user_id classified tweet_id.
        This method updates the score of the item in the priority queue and adds the user_id to the tweet's redis set

        :param tweet_id:
        :param user_id:
        """
        if not self.pq:
            report_error(self.logger, msg='Priority queue does not exist. Aborting.')
            return
        if not self.pq.exists(tweet_id):
            # This may happen relatively often when multiple people are working on the same tweet
            report_error(self.logger, msg='Key {} does not exist anymore. Aborting.'.format(tweet_id), level='warning')
            return
        # Change priority in queue
        self.pq.increment_priority(tweet_id)
        # remove from pqueue if below certain threshold
        score = self.pq.get_score(tweet_id)
        if score >= self.priority_threshold:
            self.remove(tweet_id)
            return
        # add user to set of tweet_id
        self.rset.add(tweet_id, user_id)

    def remove(self, tweet_id):
        """Remove a tweet from Redis set, PQueue and TweetStore"""
        if self.pq.exists(tweet_id):
            self.logger.debug('Removing tweet_id {} from priority queue'.format(tweet_id))
            self.pq.remove(tweet_id)
        else:
            self.logger.debug('Tweet_id {} not found in priority queue, therefore not removed'.format(tweet_id))
        self.rset.remove(tweet_id)
        self.tweet_store.remove(tweet_id)

    def flush(self):
        """Self-destroy and clean up all keys"""
        self.pq.self_remove()
        self.rset.self_remove_all()
        self.tweet_store.remove_all()


class RedisSet(Redis):
    def __init__(self, project, namespace='cb', key_namespace='tweet_id', **args):
        super().__init__(self)
        # logging
        self.logger = logging.getLogger('RedisSet')

        self.project = project
        self.namespace = namespace
        self.key_namespace = key_namespace

    def key(self, set_key):
        return "{}:{}:{}:{}".format(self.namespace, self.key_namespace, self.project, set_key)

    def add(self, set_key, value):
        self._r.sadd(self.key(set_key), value)

    def is_member(self, set_key, value):
        return self._r.sismember(self.key(set_key), value)

    def remove(self, set_key):
        self._r.delete(self.key(set_key))

    def num_members(self, set_key):
        return self._r.scard(self.key(set_key))

    def print_members(self, set_key):
        key = self.key(set_key)
        if not self._r.exists(key):
            print('Key {} is empty.'.format(key))
            return
        output = 'Members of key {}:\n'.format(key)
        count = 1
        for item in self._r.sscan_iter(key):
            output += "{:02d}) {}\n".format(count, item.decode())
            count += 1
        print(output)

    def self_remove_all(self):
        for k in self._r.scan_iter(self.key('*')):
            self._r.delete(k)
