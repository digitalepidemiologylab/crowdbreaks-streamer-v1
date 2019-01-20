from app.settings import Config
from app.utils.redis import Redis
import redis
from datetime import datetime, timedelta
from itertools import cycle, dropwhile


class RedisS3Queue(Redis):
    """
    Handles a queue of tweets for each project to be uploaded to S3 by a celery beat task. 
    Additionally it keeps track of daily and hourly counts for stats.
    """
    def __init__(self):
        super().__init__()
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.counts_namespace = 'counts'

    def queue_key(self, project):
        return "{}:{}:{}".format(self.namespace, self.config.REDIS_STREAM_QUEUE_KEY, project)

    def count_key(self, project, day, hour):
        return "{}:{}:{}:{}:{}".format(self.config.REDIS_NAMESPACE, self.counts_namespace, project, day, hour)

    def push(self, tweet, project):
        self.update_counts(project)
        self._r.rpush(self.queue_key(project), tweet)

    def pop(self, project):
        return self._r.lpop(self.queue_key(project))

    def pop_all(self, key):
        pipe = self._r.pipeline()
        res = pipe.lrange(key, 0, -1).delete(key).execute()
        return res[0]

    def num_elements_in_queue(self, key):
        return self._r.llen(key)

    def find_projects_in_queue(self):
        keys = []
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.config.REDIS_STREAM_QUEUE_KEY)):
            keys.append(key)
        return keys

    def clear_queue(self):
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.config.REDIS_STREAM_QUEUE_KEY)):
            self._r.delete(key)

    def get_counts(self, project, day=None, hour=None):
        if day is None:
            day = self._get_today()
        if hour is not None:
            key = self.count_key(project, day, hour)
            counts = self._r.get(key)
            if counts is None:
                return 0
            else:
                return int(counts.decode())
        # if hour is not given, return daily counts
        hour = self._get_hour()
        counts = 0
        for h in self.full_day_hour_range():
            key = self.count_key(project, day, h)
            c = self._r.get(key)
            if c is not None:
                counts += int(c.decode())
        return counts

    def update_counts(self, project, day=None, hour=None, incr=1):
        if day is None:
            day = self._get_today()
        if hour is None:
            hour = self._get_hour()
        counts = self.get_counts(project, day, hour)
        key = self.count_key(project, day, hour)
        self._r.set(key, counts + incr)
    
    def clear_counts(self, older_than=90):
        """Clear all keys for project older than `older_than` days"""
        end_day = datetime.now()
        start_day = end_day - timedelta(days=older_than)
        except_dates = list(self.daterange(start_day, end_day))
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.counts_namespace)):
            day = key.decode().split(':')[-2]
            if day not in except_dates:
                self._r.delete(key)

    def clear_all_counts(self):
        for key in self._r.scan_iter("{}:{}:*".format(self.config.REDIS_NAMESPACE, self.counts_namespace)):
            self._r.delete(key)

    def daterange(self, date1, date2, hourly=False):
        while date1 <= date2:
            if hourly:
                yield date1.strftime('%Y-%m-%d:%H')
                date1 += timedelta(hours=1)
            else:
                yield date1.strftime('%Y-%m-%d')
                date1 += timedelta(days=1)

    def full_day_hour_range(self):
        for n in range(0, 24):
            yield str(n).zfill(2)

    def clear(self):
        self.clear_all_counts()
        self.clear_queue()


    # private methods

    def _get_today(self):
        now = datetime.now()
        return now.strftime("%Y-%m-%d")

    def _get_hour(self):
        now = datetime.now()
        return now.strftime("%H")
