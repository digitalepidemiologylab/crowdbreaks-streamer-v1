from app.settings import Config
from app.utils.redis import Redis

class ESQueue(Redis):
    """Multi-indexing using Elasticsearch bulk API"""

    def __init__(self):
        super().__init__()
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE

    def queue_key(self, project):
        return "{}:{}:{}".format(self.namespace, self.config.ES_QUEUE_KEY, project)

    def find_projects_in_queue(self):
        keys = []
        for key in self._r.scan_iter("{}:{}:*".format(self.namespace, self.config.ES_QUEUE_KEY)):
            keys.append(key)
        return keys

    def num_elements_in_queue(self, key):
        return self._r.llen(key)

    def push(self, doc, project):
        self._r.rpush(self.queue_key(project), doc)

    def pop_all(self, key):
        pipe = self._r.pipeline()
        res = pipe.lrange(key, 0, -1).delete(key).execute()
        return res[0]

    def clear(self):
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.config.ES_QUEUE_KEY)):
            self._r.delete(key)
