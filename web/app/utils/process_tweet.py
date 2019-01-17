import math
import re
import logging
from app.utils.predict_sentiment import PredictSentiment
from helpers import report_error

class ProcessTweet(object):
    """Wrapper class for functions to process/modify tweets"""

    # Fields to extract from tweet object
    KEEP_FIELDS = [
            'id', 
            'created_at',
            'text',
            'lang',
            'coordinates',
            'timestamp_ms', 
            {
                'place': [
                    'id',
                    'place_type',
                    'full_name',
                    'country',
                    'country_code',
                    'place_type'
                ]
            },
            {
                'entities': ['hashtags']
            },
            {
                'user': [
                    'description',
                    'screen_name',
                    'id_str',
                    'lang',
                    'name',
                    'location',
                    'time_zone',
                    'geo_enabled'
                ]
            }
        ]


    def __init__(self, project=None, tweet=None):
        self.tweet = tweet            # initial tweet
        self.processed_tweet = None   # processed tweet
        self.logger = logging.getLogger(__name__)
        self.project = project

    @property
    def is_retweet(self):
        return 'retweeted_status' in self.tweet

    def process(self):
        # reduce to only certain fields
        self.strip()
        # add is_retweet field
        self.add_retweet_info()
        # compute average location from bounding box (reducing storage on ES)
        if self.tweet['place'] is not None and self.tweet['place']['bounding_box'] is not None:
            self.compute_average_location()
            self.logger.debug('Computed average location {} and average radius {}'.format(self.processed_tweet['place']['average_location'], 
                self.processed_tweet['place']['location_radius']))
        if self.project == 'vaccine-sentiment-tracking' and 'text' in self.KEEP_FIELDS:
            ps = PredictSentiment()
            model = 'fasttext_v1.ftz'
            prediction = ps.predict(self.processed_tweet['text'], model=model)
            if prediction is not None:
                meta = {'sentiment': {str(model.split('.')[0]): {'label': prediction['labels'][0], 'label_val': prediction['label_vals'][0], 'probability': prediction['probabilities'][0]}}}
                self.logger.debug('meta: {}'.format(meta))
                self.add_meta(meta)
        return self.get_processed_tweet()

    def strip(self):
        """Strip fields before sending to ElasticSearch 
        """
        tweet_stripped = {}
        if self.processed_tweet is not None:
            tweet_stripped = self.processed_tweet
        for key in self.KEEP_FIELDS:
            if isinstance(key, dict):
                nested_key, nested_values = list(key.items())[0]
                if nested_key in self.tweet and self.tweet[nested_key] is not None:
                    for val in nested_values:
                        if val in self.tweet[nested_key] and self.tweet[nested_key][val] is not None:
                            try:
                                tweet_stripped[nested_key][val] = self.tweet[nested_key].get(val, None)
                            except KeyError:
                                tweet_stripped[nested_key] = {}
                                tweet_stripped[nested_key][val] = self.tweet[nested_key].get(val, None)
            else:
                tweet_stripped[key] = self.tweet.get(key, None)
        if 'text' in self.KEEP_FIELDS:
            tweet_stripped['text'] = self._get_text()
        self.processed_tweet = tweet_stripped

    def compute_average_location(self):
        """Compute average location from bounding box
        """
        if self.tweet is None:
            return None
        coords = self.tweet.get('place', {}).get('bounding_box', {}).get('coordinates', None)
        if coords is None:
            return
        parsed_coords = []
        for lon_d, lat_d in coords[0]:
            parsed_coords.append([float(lon_d), float(lat_d)])
        av_x = av_y = av_z = 0.0
        for lon_d, lat_d in parsed_coords:
            # convert to radian
            lon = lon_d * math.pi / 180.0
            lat = lat_d * math.pi / 180.0
            # transform to cartesian coords and sum up
            av_x += math.cos(lat) * math.cos(lon)
            av_y += math.cos(lat) * math.sin(lon)
            av_z += math.sin(lat)
        # normalize
        num_points = len(parsed_coords)
        av_x /= num_points
        av_y /= num_points
        av_z /= num_points
        # transform back to polar coordinates
        av_lat = (180 / math.pi) * math.atan2(av_z, math.sqrt(av_x * av_x + av_y * av_y))
        av_lon = (180 / math.pi) * math.atan2(av_y, av_x)
        # calculate approximate radius if polygon is approximated to be a circle (for better estimate, calculate area)
        max_lat = max([lat for lon, lat in parsed_coords])
        max_lon = max([lon for lon, lat in parsed_coords])
        radius = (abs(av_lon - max_lon) + abs(av_lat - max_lat))/2
        # store in target object
        if 'place' not in self.processed_tweet:
            self.processed_tweet['place'] = {}
        self.processed_tweet['place']['average_location'] = [av_lon, av_lat]
        self.processed_tweet['place']['location_radius'] = radius


    def add_meta(self, meta):
        if self.processed_tweet is None:
            self.error('Cannot add meta to empty tweet.')
            return
        if 'meta' not in self.processed_tweet:
            self.processed_tweet['meta'] = {}
        if not isinstance(meta, dict):
            self.error('To be added meta must be a dictionary.')
        # merge with existing meta
        self.processed_tweet['meta'] = {**self.processed_tweet['meta'], **meta}

    def add_retweet_info(self):
        if self.tweet is None:
            return
        self.processed_tweet['is_retweet'] = self.is_retweet
        
    def get_processed_tweet(self):
        """get_processed_tweet"""
        if self.tweet is None:
            return None
        if self.processed_tweet is None:
            return self.tweet
        else:
            return self.processed_tweet

    def error(self, msg):
        report_error(self.logger, msg)

    # private methods

    def _get_text(self):
        if self.is_retweet:
            prefix = self._get_retweet_prefix()
            return prefix + self._get_full_text(self.tweet['retweeted_status'])
        else:
            return self._get_full_text(self.tweet)

    def _get_full_text(self, tweet_obj):
        if 'extended_tweet' in tweet_obj:
            return tweet_obj['extended_tweet']['full_text']
        else:
            return tweet_obj['text']

    def _get_retweet_prefix(self):
        m = re.match(r'^RT (@\w+): ', self.tweet['text'])
        try:
            return m[0]
        except TypeError:
            return ''
