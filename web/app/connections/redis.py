import redis
import logging
import os
from flask import current_app as app

POOL = None

class Redis():
    """Simple Redis wrapper to handle connection pools
    """

    def __init__(self, app=None, logger=None):
        self.logger = logger
        if self.logger is None:
            self.logger = logging.getLogger('Redis')

    @property
    def redis_conn(self):
        return self._get_connection()

    @property
    def pool(self):
        global POOL
        if POOL is None:
            pw = None
            if 'REDIS_PW' in app.config:
                if len(app.config['REDIS_PW']) > 0:
                    pw = app.config['REDIS_PW']

            POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
                    port=app.config['REDIS_PORT'],
                    db=app.config['REDIS_DB'],
                    password=pw)
            self.logger.info('Created new Redis connection pool {}'.format(POOL))
        return POOL

    def init_app(self, app):
        """Called from application factory"""
        pass

    def test_connection(self):
        test = self.redis_conn.ping()
        if test:
            self.logger.info('Successfully connected to Redis host {}'.format(app.config['REDIS_HOST']) )
        else:
            self.logger.error('FAILURE: Connection to Redis host {} not successful'.format(app.config['REDIS_HOST']))
        return test

    def _get_connection(self):
        return redis.StrictRedis(connection_pool=self.pool)
        # return redis.StrictRedis(host=self.host, port=self.port)

    def blpop(self, q_list):
        return self.redis_conn.blpop(q_list)

    def rpush(self, q, obj):
        self.redis_conn.rpush(q, obj)
