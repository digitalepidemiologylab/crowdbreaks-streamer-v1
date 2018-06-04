import sys 
sys.path.append('..')
sys.path.append('../web/')
from web.app.connections import elastic
import logging.config

import random
import time
from datetime import datetime
import string
from faker import Faker


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
    f = Faker()
    profile = f.profile()
    t = { 
            'id': int(timestamp*100000000),
            'created_at': time_now,
            'project': INDEX,
            'text': f.text(),
            'lang': 'en',
            'coordinates': {
                'coordinates': get_random_lon_lat(),
                'type': "Point"
                },
            'timestamp_ms': str(int(timestamp*1000)),
            'user': {
                'description': f.text(),
                'screen_name': profile['username'],
                'id_str': str(int(timestamp*100000)),
                'lang': 'en',
                'name': profile['name'],
                'location': f.country(),
                'time_zone': f.timezone(),
                'geo_enabled': True,
                },
            "entities": {
                "hashtags": [{ "indices": [71, 75], "text": f.word() }]
                },
            "place": {
                "country_code": f.country_code(),
                "country": f.country(),
                "full_name": f.city() + ' ' + f.country(),
                "average_location": get_random_lon_lat(),
                "place_type": "city",
                "id": generate_random_text(words=1),
                "location_radius": random.random()
                }
            }

    if INDEX == 'project_vaccine_sentiment':
        predictions = {0: 'pro-vaccine', 1: 'anti-vaccine', 2:'neutral'}
        sentiment = {str(MODEL.split('.')[0]): {'label': predictions[random.randint(0,2)], 'probability': random.random(), 'label_val': int(random.randint(-1,1))}}
        t['meta'] = {'sentiment': sentiment}
        t['sentiment'] = sentiment
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
    NUM_TWEETS=200

    logger.info('Generating random tweets and index them to index {}'.format(INDEX))

    if INDEX in es_client.list_templates():
        yes_no = input('Index {} already exists for. Delete an recreate? (y/n)'.format(INDEX))
        if yes_no == 'y':
            es_client.delete_template('project')
            es_client.delete_index(INDEX)
            es_client.create_index(INDEX)
    main()
