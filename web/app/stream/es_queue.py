from app.settings import Config
from app.utils.redis import Redis
import time
import os
import fcntl
import json

class ESQueue(Redis):
    """Multi-indexing using Elasticsearch bulk API"""

    def __init__(self):
        super().__init__()
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        today = time.strftime('%Y-%m-%d')
        self.dump_fname = os.path.join(self.config.PROJECT_ROOT, 'logs', 'es_bulk_indexing_errors', f'{today}.jsonl')

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

    def dump_to_disk(self, data):
        """Dump documents to disk when bulk indexing fails"""
        data = ''.join([json.dumps(d) + '\n' for d in data])
        with open(self.dump_fname, 'a') as f:
            # activate file lock
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(data)
            # release file lock
            fcntl.flock(f, fcntl.LOCK_UN)
