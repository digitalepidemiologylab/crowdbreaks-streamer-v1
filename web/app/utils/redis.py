import redis
import logging
import os
from helpers import report_error
import json

logger = logging.getLogger(__name__)

class Redis():
    def __init__(self, logger=None, connection=None, **kwargs):
        self.host = os.environ.get('REDIS_HOST', 'localhost')
        self.port = os.environ.get('REDIS_PORT', 6379)
        self.db = os.environ.get('REDIS_DB', 0)
        self.connection = connection

    @property
    def _r(self):
        if self.connection is None:
            self.connection = redis.StrictRedis(host=self.host, port=self.port)
        return self.connection

    def get_connection(self):
        return self._r

    def test_connection(self):
        test = self._r.ping()
        if test:
            logger.info(f'Successfully connected to Redis host {self.host}:{self.port}')
        else:
            report_error(logger, msg='FAILURE: Connection to Redis host {self.host}:{self.port} not successful')
        return test

    def set_cached(self, key, data, expire_in_min=1):
        self._r.set(key, json.dumps(data).encode(), ex=expire_in_min*60)

    def get_cached(self, key):
        return json.loads(self._r.get(key))

    def exists(self, key):
        return self._r.exists(key)

