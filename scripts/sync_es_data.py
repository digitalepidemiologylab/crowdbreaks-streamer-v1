"""
Script to sync data between two Elasticsearch indices. Note that the indices must have a suitable mapping
"""

from utils import get_es_client
import json
import os
from elasticsearch import helpers
from datetime import datetime
import logging
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

def load_docs(es, args):
    query = {'query': {'range': {'created_at': {'gte': args.since, 'lte': args.until}}}}
    scan = helpers.scan(es.es, scroll='1m', query=query, index=args.index, doc_type=args.doc_type)
    for item in scan:
        yield {'_index': args.index, '_type': args.doc_type, '_id': item['_id'], '_source': item['_source']}

def main(args):
    es_prd = get_es_client(env='prd')
    if not es_prd.test_connection():
        raise Exception('Connection issue')

    # bulk index on development
    es_dev = get_es_client(env='dev')
    try:
        resp = helpers.bulk(es_dev.es, load_docs(es_prd, args))
    except Exception as e:
        logger.error('Bulk indexing not unsuccessful!')
        print(e)
    else:
        logger.info(f'Successfully indexed {resp[0]:,} docs...')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', required=True, type=str, help='Name of index')
    parser.add_argument('-d', '--doc-type', dest='doc_type', default='tweet', required=False, type=str, help='Doc type')
    parser.add_argument('-s', '--since', default='now-1h', required=False, type=str, help='Since')
    parser.add_argument('-u', '--until', default='now', required=False, type=str, help='Until')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
