"""
Script to download certain data by query
"""

from utils import get_es_client
import json
import os
from elasticsearch import helpers
from datetime import datetime
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

# VARS
MATCH_QUERY = 'snake'
INDEX = 'project_wuhan'
DOC_TYPE = 'tweet'
QUERY = {'query': {'bool': {'must': [{'match_phrase': {'text': MATCH_QUERY}}]}},
        'aggs': {'counts': {'date_histogram': {'field': 'created_at', 'interval': 'hour', 'format': 'yyyy-MM-dd HH:mm:ss'}}}
        }

def load_docs(es):
    res = es.es.search(index=INDEX, body=QUERY, filter_path=['aggregations.counts'])
    return res

def main():
    es_prd = get_es_client(env='prd')
    if not es_prd.test_connection():
        raise Exception('Connection issue')

    # bulk index on development
    resp = load_docs(es_prd)
    resp = resp['aggregations']['counts']['buckets']
    df = pd.DataFrame(resp)
    df['timestamp'] = pd.to_datetime(df['key_as_string'])
    df = df.drop(['key', 'key_as_string'], axis=1)
    df.to_csv(f'{MATCH_QUERY}.csv', index=False)


if __name__ == "__main__":
    main()
