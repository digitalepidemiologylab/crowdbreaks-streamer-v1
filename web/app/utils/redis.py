import redis
import logging
import os
from helpers import report_error
import json

class Redis():
    def __init__(self, logger=None):
        self.host = os.environ.get('REDIS_HOST', 'localhost')
        self.port = os.environ.get('REDIS_PORT', 6379)
        self.db = os.environ.get('REDIS_DB', 0)
        self.logger = logging.getLogger('Redis')
        self.connection = None

    @property
    def _r(self):
        if self.connection is None:
            self.connection = redis.StrictRedis(host=self.host, port=self.port)
        return self.connection

    def test_connection(self):
        test = self._r.ping()
        if test:
            self.logger.info('Successfully connected to Redis host {}:{}'.format(self.host, self.port))
        else:
            report_error(self.logger, msg='FAILURE: Connection to Redis host {}:{} not successful'.format(self.host, self.port))
        return test

    def set_cached(self, key, data, expire_in_min=1):
        self._r.set(key, json.dumps(data).encode(), ex=expire_in_min*60)

    def get_cached(self, key):
        return json.loads(self._r.get(key))

    def exists(self, key):
        return self._r.exists(key)

