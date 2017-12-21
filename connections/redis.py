import redis
import instance.config
from logger import Logger

POOL = None

class Redis():
    """Simple Redis wrapper to handle connection pools

    Proper usage:
    > r = Redis()
    > r.init()             # Initialize connection pools in the beginning
    > r.test_connection()  # True
    """

    def __init__(self, app=None, logger=None):
        self.logger = logger
        if self.logger is None:
            self.logger = Logger.setup('Redis')

    @property
    def redis_conn(self):
        return self._get_connection()

    def init(self, connection_pool=None):
        # declare globals
        global POOL

        # avoid running this multiple times
        if POOL is not None:
            return

        if connection_pool is not None:
            POOL = connection_pool
            return

        pw = None
        if 'REDIS_PW' in instance.config.__dict__:
            if len(instance.config.REDIS_PW) > 0:
                pw = instance.config.REDIS_PW

        POOL = redis.ConnectionPool(host=instance.config.REDIS_HOST,
                port=instance.config.REDIS_PORT,
                db=instance.config.REDIS_DB,
                password=pw)

        self.logger.info('Created new Redis connection pool {}'.format(POOL))

    def test_connection(self):
        test = self.redis_conn.ping()
        if test:
            self.logger.info('Successfully connected to Redis host {}'.format(instance.config.REDIS_HOST) )
        else:
            self.logger.error('FAILURE: Connection to Redis host {} not successful'.format(instance.config.REDIS_HOST))
        return test

    def _get_connection(self):
        return redis.StrictRedis(connection_pool=POOL)

    def blpop(self, q_list):
        return self.redis_conn.blpop(q_list)

    def rpush(self, q, obj):
        self.redis_conn.rpush(q, obj)


