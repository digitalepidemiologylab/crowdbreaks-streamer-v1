class ProcessTweet(object):
    """Wrapper class for processing Tweets """

    # Fields to extract from tweet object
    KEEP_FIELDS = [ 'id', 
            'created_at',
            'project',
            'text', {
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

