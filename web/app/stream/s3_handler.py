import redis
from app.settings import Config
import boto3
import logging


class S3Handler():
    """Handles queuing in Redis and pushing tweets to S3"""

    def __init__(self):
        self.config = Config()
        self.redis = redis.Redis(host=self.config.REDIS_HOST, port=self.config.REDIS_PORT, db=self.config.REDIS_DB)
        if not self.redis.ping():
            raise Exception("Could not connect to Redis instance on {}.".format(self.config.REDIS_HOST))
        self.bucket = self.config.S3_BUCKET
        self.logger = logging.getLogger(__name__)

    def queue_key(self, project):
        return "{}:{}:{}".format(self.config.REDIS_NAMESPACE, self.config.REDIS_STREAM_QUEUE_KEY, project)

    def push_to_queue(self, tweet, project):
        self.redis.rpush(self.queue_key(project), tweet)

    def pop_from_queue(self, project):
        return self.redis.lpop(self.queue_key(project))

    def pop_all(self, key):
        pipe = self.redis.pipeline()
        res = pipe.lrange(key, 0, -1).delete(key).execute()
        return res[0]

    def num_elements_in_queue(self, key):
        return self.redis.llen(key)

    def find_projects_in_queue(self):
        keys = []
        for key in self.redis.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.config.REDIS_STREAM_QUEUE_KEY)):
            keys.append(key)
        return keys

    def upload_to_s3(self, content, key):
        try: 
            self._s3_client.put_object(Body=content, Bucket=self.bucket, Key=key)
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            return True

    def list_buckets(self):
        return self._s3_client.list_buckets()
        
    # private methods

    @property
    def _s3_client(self):
        return boto3.client('s3')
