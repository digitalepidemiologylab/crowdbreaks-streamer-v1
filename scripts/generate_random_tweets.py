import sys 
sys.path.append('..')
sys.path.append('../web/')
from web.app.connections import elastic
import logging.config

import random
import time
from datetime import datetime
import string


def get_random_lon_lat():
    return [random.uniform(-180,180), random.uniform(-90, 90)]

def generate_random_text(words=5):
    s = ''
    for _ in range(words):
        s += ''.join([random.choice(string.ascii_letters) for _ in range(random.randint(1,10))])
        s += ' '
    return s.strip()

def generate_fake_tweet():
    twitter_time_format = '%a %b %d %H:%M:%S +0000 %Y'
    time_now = datetime.utcnow().strftime(twitter_time_format)
    timestamp = time.time()
    t = { 
            'id': int(timestamp*100000000),
            'created_at': time_now,
            'project': INDEX,
            'text': generate_random_text(words=10),
            'lang': 'en',
            'coordinates': {
                'coordinates': get_random_lon_lat(),
                'type': "Point"
                },
            'timestamp_ms': str(int(timestamp*1000)),
            'user': {
                'description': generate_random_text(words=4),
                'screen_name': generate_random_text(words=1),
                'id_str': str(int(timestamp*100000)),
                'lang': 'en',
                'name': generate_random_text(words=2),
                'location': generate_random_text(words=1),
                'time_zone': 'Eastern Time (US & Canada)',
                'geo_enabled': True,
                },
            "entities": {
                "hashtags": [{ "indices": [71, 75], "text": "random" }]
                },
            "place": {
                "country_code": "US",
                "country": "United States",
                "full_name": generate_random_text(words=2),
                "average_location": get_random_lon_lat(),
                "place_type": "city",
                "id": generate_random_text(words=1),
                "location_radius": random.random()
                }
            }

    if INDEX == 'project_vaccine_sentiment':
        predictions = {0: 'pro-vaccine', 1: 'anti-vaccine', 2:'neutral'}
        sentiment = {'sentiment': {str(MODEL.split('.')[0]): {'label': predictions[random.randint(0,2)], 'probability': random.random()}}}
        t['meta'] = sentiment
    return t


def main():
    for _ in range(NUM_TWEETS):
        t = generate_fake_tweet()
        es_client.index_tweet(t)
    return


if __name__ == "__main__":
    # logging
    logging.config.fileConfig('script_logging.conf')
    logger = logging.getLogger('script')

    # initialize ES
    es_client = elastic.Elastic()
    es_client.test_connection()

    # global vars
    INDEX = 'project_vaccine_sentiment'
    DOC_TYPE='tweet'
    MODEL='fasttext_v1'
    NUM_TWEETS=20                             # num tweets to generate

    logger.info('Generating random tweets and index them to index {}'.format(INDEX))

    es_client.delete_index(INDEX)
    es_client.create_index(INDEX)
    main()
