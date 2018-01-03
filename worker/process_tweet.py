import pdb
import math
from nltk import TweetTokenizer
import re

class ProcessTweet(object):
    """Wrapper class for processing Tweets """

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


    def __init__(self):
        pass

    @classmethod
    def strip(cls, tweet):
        """Strip fields before sending to ElasticSearch 

        :tweet: Tweet object
        :returns: Stripped tweet
        """

        tweet_stripped = {}
        for key in cls.KEEP_FIELDS:
            if isinstance(key, dict):
                nested_key, nested_values = list(key.items())[0]
                if nested_key in tweet and tweet[nested_key] is not None:
                    for val in nested_values:
                        if val in tweet[nested_key] and tweet[nested_key][val] is not None:
                            try:
                                tweet_stripped[nested_key][val] = tweet[nested_key].get(val, None)
                            except KeyError:
                                tweet_stripped[nested_key] = {}
                                tweet_stripped[nested_key][val] = tweet[nested_key].get(val, None)
            else:
                tweet_stripped[key] = tweet.get(key, None)

        return tweet_stripped

    @classmethod
    def compute_average_location(cls, tweet, target_tweet):
        """Compute average location from bounding box

        :tweet: Tweet object with place field
        :target_tweet: (Stripped) tweet object to write into
        :returns: Modified target_tweet
        """

        coords = tweet.get('place', {}).get('bounding_box', {}).get('coordinates', None)

        if coords is None:
            return target_tweet

        av_x = av_y = av_z = 0
        for lon_d, lat_d in coords[0]:
            # convert to radian
            lon = lon_d * math.pi / 180.0
            lat = lat_d * math.pi / 180.0

            # transform to cartesian coords and sum up
            av_x += math.cos(lat) * math.cos(lon)
            av_y += math.cos(lat) * math.sin(lon)
            av_z += math.sin(lat)

        # normalize
        num_points = len(coords[0])
        av_x /= num_points
        av_y /= num_points
        av_z /= num_points

        # transform back to polar coordinates
        av_lat = (180 / math.pi) * math.atan2(av_z, math.sqrt(av_x * av_x + av_y * av_y))
        av_lon = (180 / math.pi) * math.atan2(av_y, av_x)

        # calculate approximate radius if polygon is approximated to be a circle (for better estimate, calculate area)
        max_lat = max([lat for lon, lat in coords[0]])
        max_lon = max([lon for lon, lat in coords[0]])
        radius = (abs(av_lon - max_lon) + abs(av_lat - max_lat))/2

        # store in target object
        target_tweet['place']['average_location'] = [av_lon, av_lat]
        target_tweet['place']['location_radius'] = radius

        return target_tweet

    @classmethod
    def tokenize(self, tweet, discard_word_length=2):
        """Prepare tweets for sentence embeddings

        :tweet: input tweet text
        :discard_word_length: Discard tweets with less words than this
        :returns: Same tweet with text_tokenized field. Returns None if tweet is invalid.

        """

        tknzr = TweetTokenizer()

        # Replace unnecessary spacings/EOL chars
        try:
            tweet = tweet.replace('\n', '').replace('\r', '').strip()
        except:
            return None
        tweet = tknzr.tokenize(tweet)

        # throw away anything below 2 words
        if not  discard_word_length < len(tweet) < 110:
            return None
        tweet = ' '.join(tweet)
        tweet = tweet.lower()

        # replace urls and mentions
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))','<url>',tweet)
        tweet = re.sub('(\@[^\s]+)','<user>',tweet)
        try:
            tweet = tweet.decode('unicode_escape').encode('ascii','ignore')
        except:
            pass
        filter(lambda word: ' ' not in word, tweet)
        return tweet

