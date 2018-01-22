import redis
import pdb
import os
from random import randint
import logging


class Redis():
    def __init__(self, logger=None):
        self.host = os.environ.get('REDIS_HOST', 'localhost')
        self.port = os.environ.get('REDIS_PORT', 6379)
        self.db = os.environ.get('REDIS_DB', 0)
        self.logger = logging.getLogger('Redis')

    @property
    def _r(self):
        return self._get_connection()

    def _get_connection(self):
        # return redis.StrictRedis(host=self.host, port=self.port, db=self.db)
        return redis.StrictRedis(host=self.host, port=self.port)

    def test_connection(self):
        test = self._r.ping()
        if test:
            self.logger.info('Successfully connected to Redis host {}:{}'.format(self.host, self.port))
        else:
            self.logger.error('FAILURE: Connection to Redis host {}:{} not successful'.format(self.host, self.port))
        return test


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

    def __str__(self):
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
        return self._r.exists(self.key)

    @property
    def key(self):
        return "{}:{}:{}".format(self.namespace, self.key_namespace, self.project)

    def add(self, value, priority=0):
        """Push value with given priority to queue. Enforce max length of queue by removing low-priority elements."""
        while len(self) >= self.MAX_QUEUE_LENGTH:
            self.remove_lowest_priority()
        self._r.zadd(self.key, priority, value)

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

    def increment_priority(self, val, incr=1):
        if self._r.zrank(self.key, val) is None:
            self.logger.debug("Priority of value {} cannot be changed, because it doesn't exist anymore".format(val))
        self._r.zincrby(self.key, val, amount=incr)

    def remove_lowest_priority(self, random_deletion=True):
        """Remove key with the lowest priority"""
        if not random_deletion:
            # Just delete lowest priority key
            if self._r.zremrangebyrank(self.key, 0, 0) == 0:
                self.logger.warning('Tried to remove lowest ranking element but queue is empty.')
            return

        # Remove a random lowest priority key
        items = self._r.zrevrange(self.key, 0, 0, withscores=True)
        if len(items) == 0:
            self.logger.warning('Tried to remove lowest ranking element but queue is empty.')
            return
        lowest_score = items[0][1]
        num_elements = self._r.zcount(self.key, lowest_score, lowest_score)
        if num_elements == 0:
            self.logger.error('Element with score {} could not be found. Possibly it has been removed before. Aborting.'.format(lowest_score))
            return
        elif num_elements == 1:
            self._r.zremrangebyrank(self.key, 0, 0)
        else:
            # multiple elements with the same lowest score, randomly remove one
            rand_index = randint(0, num_elements-1)
            self.logger.debug('Picked {} as randindex between {} and {}'.format(rand_index, 0, num_elements-1))
            res = self._r.zremrangebyrank(self.key, rand_index, rand_index)
            if res != 1:
                self.logger.error('Random key could not be deleted because it does not exist anymore')


    def remove(self, item):
        """Remove key by keyname"""
        if self._r.zrem(self.key, item) == 0:
            self.logger.error('Element {} could not be deleted'.format(item.decode()))

    def get_score(self, val):
        return self._r.zscore(self.key, val)

    def self_remove(self):
        self._r.delete(self.key)

    def exists(self, val):
        if self._r.zrank(self.key, val) is None:
            return False
        else:
            return True


class TweetIdQueue:
    """Handles Tweet IDs in a priority queue and keeps a record of which user classified what tweet as a set in Redis."""

    def __init__(self, project, namespace='cb', logger=None, priority_threshold=3, **kwargs):
        """__init__

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
        self.priority_threshold = priority_threshold

    def add(self, tweet_id, priority=0):
        """Simply adds a new tweet_id to its priority queue"""
        self.pq.add(tweet_id, priority=priority)

    def get(self, user_id=None):
        """Get new tweet to classify for user

        :param user_id:
        """

        # If no user is defined, simply pop the queue
        if user_id is None:
            item = self.pq.pop()
            if item is None:
                self.logger.warning('Queue is empty')
                return None
            else:
                return item
        else:
            num = 3
            starts = [num*i for i in range(1 + (len(self.pq)//num))]
            for s in starts:
                item_range = self.pq._r.zrevrangebyscore(self.pq.key, '+inf', '-inf', start=s, num=num, withscores=True)
                for item, score in item_range:
                    if not self.rset.is_member(item.decode(), user_id):
                        return item.decode()
            self.logger.warning('No new tweet could be found for user_id {}'.format(user_id))


    def update(self, tweet_id, user_id):
        """Track the fact that user user_id classified tweet_id. 
        This method updates the score of the item in the priority queue and adds the user_id to the tweet's redis set

        :param tweet_id:
        :param user_id:
        """
        if not self.pq:
            self.logger.error('Priority queue does not exist. Aborting.')
            return
        
        if not self.pq.exists(tweet_id):
            # This may happen relatively often when multiple people are working on the same tweet
            self.logger.warning('Key {} does not exist anymore. Aborting.'.format(tweet_id))
            return

        # Change priority in queue
        self.pq.increment_priority(tweet_id)

        # remove from pqueue if below certain threshold
        score = self.pq.get_score(tweet_id)
        if score >= self.priority_threshold:
            self.pq.remove(tweet_id)
            self.rset.remove(tweet_id)
            self.logger.debug('Priority threshold reached, getting rid of tweet_id {}'.format(tweet_id))
            return

        # add user to set of tweet_id
        self.rset.add(tweet_id, user_id)

    def flush(self):
        """Self-destroy and clean up all keys"""
        self.pq.self_remove()
        self.rset.self_remove_all()


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


