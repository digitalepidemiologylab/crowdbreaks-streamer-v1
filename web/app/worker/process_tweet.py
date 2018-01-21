import pdb
import math
from nltk import TweetTokenizer
import re
import logging

class ProcessTweet(object):
    """Wrapper class for functions to process/modify tweets"""

    # Fields to extract from tweet object
    KEEP_FIELDS = [ 'id', 
            'created_at',
            'project',
            'text',
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
            'lang',
            'coordinates',
            'timestamp_ms', {
                'entities': [
                    'hashtags',
                    ]
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


    def __init__(self, tweet=None):
        self.tweet = tweet            # initial tweet
        self.processed_tweet = None   # processed tweet
        self.logger = logging.getLogger(__name__)


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
        return

    def tokenize(self, text=None, discard_word_length=2):
        """Tokenize text for sentence embedding

        :text: input text
        :discard_word_length: Discard tweets with less words than this
        :returns: Same tweet with text_tokenized field. Returns None if tweet is invalid.

        """
        if text is None:
            if 'text' in self.tweet:
                text = self.tweet['text']
            else:
                return None

        tknzr = TweetTokenizer()

        # Replace unnecessary spacings/EOL chars
        try:
            text = text.replace('\n', '').replace('\r', '').strip()
        except:
            return None
        text = tknzr.tokenize(text)

        # throw away anything below certain words length
        if not discard_word_length < len(text) < 110:
            return None
        text = ' '.join(text)
        text = text.lower()

        # replace urls and mentions
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))','<url>',text)
        text = re.sub('(\@[^\s]+)','<user>',text)
        try:
            text = text.decode('unicode_escape').encode('ascii','ignore')
        except:
            pass
        filter(lambda word: ' ' not in word, text)
        return text.strip()

    def get_processed_tweet(self):
        if self.tweet is None:
            return None
        if self.processed_tweet is None:
            return self.tweet
        else:
            return self.processed_tweet
