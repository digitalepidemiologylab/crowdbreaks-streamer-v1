"""
Script to sync data between two Elasticsearch indices. Note that the indices must have a suitable mapping
"""

from utils import get_es_client
import json
import os
from elasticsearch import helpers
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

# VARS
INDEX = 'trending_topics_covid'
QUERY = {"query": {"match_all": {}}}
DOC_TYPE = '_doc'
QUERY = {'query': {'range': {'created_at': {'gte': 'now-1d', 'lte': 'now'}}}}

def load_docs(es):
    scan = helpers.scan(es.es, scroll='1m', query=QUERY, index=INDEX, doc_type=DOC_TYPE)
    for item in scan:
        yield {'_index': INDEX, '_type': DOC_TYPE, '_id': item['_id'], '_source': item['_source']}

def main():
    es_prd = get_es_client(env='prd')
    if not es_prd.test_connection():
        raise Exception('Connection issue')

    # bulk index on development
    es_dev = get_es_client(env='dev')
    try:
        resp = helpers.bulk(es_dev.es, load_docs(es_prd))
    except Exception as e:
        logger.error('Bulk indexing not unsuccessful!')
        print(e)
    else:
        logger.info(f'Successfully indexed {resp[0]:,} docs...')


if __name__ == "__main__":
    main()
