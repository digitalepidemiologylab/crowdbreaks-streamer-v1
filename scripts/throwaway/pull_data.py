import sys 
sys.path.append('..')
sys.path.append('../web/')
from web.app.connections import elastic
import logging.config

import json
import os
from elasticsearch import helpers

def main():
    # res = es_client.get_geo_sentiment('project_vaccine_sentiment', limit=100)

    start_date = 'now-20y'
    end_date = 'now'
    s_date, e_date = es_client.parse_dates(start_date, end_date)
    field = 'meta.sentiment.{}.label'.format(MODEL)
    query = {
            '_source': ['place.average_location', field],
            'query': {
                'bool': {
                    'must': [
                        {'exists': {'field': 'place.average_location'}},
                        {'exists': {'field': field}},
                        {'range': {'created_at': {'gte': s_date, 'lte': e_date}}}
                        ]
                    }
                }
            }

    scan = helpers.scan(client=es_client.es, scroll='2m', query=query, index=INDEX)
    print_every = 1000
    c = 0
    res = []
    for item in scan:
        if c % print_every == 0:
            logger.info('Writing record {} ...'.format(c))
        c += 1
        res.append(item['_source'])

    logger.info('Collected {} data points'.format(len(res)))
    logger.info('Writing to file...')
    with open(os.path.join('../../crowdbreaks-extra-stuff/crowdbreaks-viz','geo_sentiment.json'), 'w') as f:
        json.dump(res, f)
    logger.info('Done!')



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

    main()
    # with open(os.path.join('../../crowdbreaks-extra-stuff/crowdbreaks-viz','geo_sentiment.json'), 'r') as f:
    #     data = json.load(f)
