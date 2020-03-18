from app.settings import Config
from app.utils.redis import Redis
import logging
import json

logger = logging.getLogger(__name__)


class PredictQueue(Redis):
    """
    Handles a queue of items to be predicted
    """
    def __init__(self, project):
        super().__init__()
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.queue_namespace = 'predict_queue'
        self.project = project

    @property
    def key(self):
        return "{}:{}:{}".format(self.namespace, self.queue_namespace, self.project)

    def __len__(self):
        return self._r.llen(self.key)

    def push(self, obj):
        obj = json.dumps(obj).encode()
        self._r.rpush(self.key, obj)

    def multi_push(self, objs):
        for obj in objs:
            self.push(obj)

    def pop_all(self):
        pipe = self._r.pipeline()
        res = pipe.lrange(self.key, 0, -1).delete(self.key).execute()
        return [json.loads(r.decode()) for r in res[0]]

    def num_elements_in_queue(self, key):
        return self._r.llen(key)

    def find_projects_in_queue(self):
        keys = []
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.config.REDIS_STREAM_QUEUE_KEY)):
            keys.append(key)
        return keys

    def clear_queue(self):
        self._r.delete(self.key)
