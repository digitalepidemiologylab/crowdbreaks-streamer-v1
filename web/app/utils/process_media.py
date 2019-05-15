from app.settings import Config
from app.stream.redis_s3_queue import RedisS3Queue
from app.stream.s3_handler import S3Handler
from app.utils.redis import Redis
from helpers import report_error
from collections import defaultdict
from datetime import datetime
import pytz
import urllib
import logging
import os

class ProcessMedia():
    """Process media (such as images) and store on S3"""

    def __init__(self, tweet, image_storage_mode='active'):
        self.tweet = tweet
        self.project = tweet['_tracking_info']['es_index_name']
        self.config = Config()
        self.namespace = self.config.REDIS_NAMESPACE
        self.tmp_path = os.path.join(self.config.PROJECT_ROOT, 'tmp')
        self.image_storage_mode = image_storage_mode
        self.redis_s3_queue = RedisS3Queue()
        self.s3 = S3Handler()
        self.logger = logging.getLogger(__name__)
        self.download_media_types = ['photo', 'animated_gif']

    def process(self):
        # Don't collect any information from retweets
        if 'retweeted_status' in self.tweet:
            self.logger.debug('Tweet is retweet.')
            return
        if self.is_possibly_sensitive and self.image_storage_mode == 'avoid_possibly_sensitive':
            self.logger.debug('Media is not collected for sensitive media in this project.')
            return

        # collect media info and update counts
        media_info = self.collect_media_info()
        if not media_info['has_media']:
            self.logger.debug('Tweet contains no media.')
            return
        for media_type, count in media_info['counts'].items():
            self.redis_s3_queue.update_counts(self.project, media_type=media_type)

        # download media and upload to S3
        idx = 0
        for media_type, urls in media_info['media_urls'].items():
            if media_type in self.download_media_types:
                for url, tweet_id, size_info in zip(urls, media_info['tweet_ids'][media_type], media_info['sizes'][media_type]):
                    f_name = self.get_f_name(url, media_type, tweet_id, idx, size_info)
                    local_path = os.path.join(self.tmp_path, f_name)
                    self.download_media(url, local_path, size_info['size'])
                    key = self.get_s3_key(f_name)
                    self.s3.upload_file(local_path, key)
                    if os.path.isfile(local_path):
                        os.remove(local_path)
                    idx += 1

    def download_media(self, url, f_name, size):
        # set format
        url = "{}:{}".format(url, size)
        try:
            urllib.request.urlretrieve(url, f_name)
        except Exception as e:
            report_error(self.logger, e)

    def get_f_name(self, url, media_type, tweet_id, idx, size_info):
        fmt = url.split('.')[-1]
        matching_keywords = self.tweet['_tracking_info']['matching_keywords']
        matching_keywords = '_'.join(matching_keywords)
        created_at = self.created_at.strftime("%Y%M%d%H%M%S")
        size = "{}-{}x{}".format(size_info['size'], size_info['w'], size_info['h'])
        return "{}-{}-{}-{}-{}-{}-{}.{}".format(created_at, self.project, tweet_id, idx, media_type, size, matching_keywords, fmt)

    def get_s3_key(self, f_name):
        return "{}/{}/{}/{}".format('media', self.project, self.created_at.strftime("%Y-%M-%d"), f_name)

    @property
    def created_at(self, fmt='%a %b %d %H:%M:%S %z %Y'):
        return datetime.strptime(self.tweet['created_at'], fmt).astimezone(pytz.utc)

    def collect_media_info(self):
        media_info = {'has_media': False,
                'counts': defaultdict(lambda: 0),
                'media_urls': defaultdict(list),
                'tweet_ids': defaultdict(list), 
                'sizes': defaultdict(list)} 
        # check both tweet and quoted tweet
        tweet_objs = [self.tweet]
        if 'quoted_status' in self.tweet:
            tweet_objs.append(self.tweet['quoted_status'])
        # collect info
        for tweet_obj in tweet_objs:
            if self._keys_exist(tweet_obj, 'extended_tweet', 'extended_entities', 'media'):
                tweet_media = tweet_obj['extended_tweet']['extended_entities']['media']
            elif self._keys_exist(tweet_obj, 'extended_entities', 'media'):
                tweet_media = tweet_obj['extended_entities']['media']
            else:
                continue
            media_info['has_media'] = True
            for m in tweet_media:
                media_info['counts'][m['type']] += 1
                # for media of type video/animated_gif media_url corresponds to a thumbnail image
                media_info['media_urls'][m['type']].append(m['media_url'])
                media_info['tweet_ids'][m['type']].append(tweet_obj['id_str'])
                for size in ['large', 'medium', 'small', 'thumb']:
                    if size in m['sizes']:
                        media_info['sizes'][m['type']].append({'size': size, 'h': m['sizes'][size]['h'], 'w': m['sizes'][size]['w']})
                        break
                else:
                    raise Exception('Could not find any size type in media')
        for _d in ['counts', 'media_urls', 'tweet_ids', 'sizes']:
            media_info[_d] = dict(media_info[_d])
        return media_info

    @property
    def is_possibly_sensitive(self):
        if 'possibly_sensitive' in self.tweet:
            return self.tweet['possibly_sensitive']
        else:
            return False


    def _keys_exist(self, element, *keys):
        """ Check if *keys (nested) exists in `element` (dict). """
        _element = element
        for key in keys:
            try:
                _element = _element[key]
            except KeyError:
                return False
        return True
