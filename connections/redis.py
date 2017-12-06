import redis
import instance.config
from logger import Logger

POOL = None
LOGGER = None

class Redis():
    """Simple Redis wrapper to handle connection pools"""

    def __init__(self):
        self.logger = LOGGER
        self.redis_conn = self._get_connection()


    def init(self, connection_pool=None):
        # declare globals
        global POOL, LOGGER

        # avoid running this multiple times
        if POOL is not None:
            return

        # initialize logger
        LOGGER = Logger.setup('Redis')
        self.logger = LOGGER

        if connection_pool is not None:
            POOL = connection_pool
            return

        if 'REDIS_PW' in instance.config.__dict__:
            POOL = redis.ConnectionPool(host=instance.config.REDIS_HOST,
                    port=instance.config.REDIS_PORT,
                    db=instance.config.REDIS_DB,
                    password=instance.config.REDIS_PW)
        else:
            POOL = redis.ConnectionPool(host=instance.config.REDIS_HOST,
                    port=instance.config.REDIS_PORT,
                    db=instance.config.REDIS_DB)

        self.logger.info('Created new connection pool {}'.format(POOL))

        # test connection
        self.redis_conn = self._get_connection()
        if self.test_connection:
            self.logger.info('Successfully connected to Redis host {}'.format(instance.config.REDIS_HOST) )
        else:
            self.logger.error('FAILURE: Connection to Redis host {} not successful'.format(instance.config.REDIS_HOST))


    def test_connection(self):
        return self.redis_conn.ping()

    def _get_connection(self):
        return redis.StrictRedis(connection_pool=POOL)

    def blpop(self, q_list):
        return self.redis_conn.blpop(q_list)

    def rpush(self, q, obj):
        self.redis_conn.rpush(q, obj)


