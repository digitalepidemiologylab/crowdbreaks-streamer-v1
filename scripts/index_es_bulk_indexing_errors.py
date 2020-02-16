"""
This script indexes all failed documents which couldn't be indexed to ES
Run this script from within <PROJECT_ROOT>/scripts
Make sure to set all global vars first!
"""

import sys
sys.path.append('..')
sys.path.append('../web/')
from web.app.connections import elastic
import requests
import json
import logging
import glob
import os
import ast

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)


def main():
    es_client = elastic.Elastic()
    if not es_client.test_connection():
        logger.error('Could not connect to Elasticsearch. Exiting.')
        sys.exit(-1)
    folder = os.path.join('..', 'web', 'logs', 'es_bulk_indexing_errors')
    f_names = glob.glob(os.path.join(folder, '*.jsonl'))
    if len(f_names) == 0:
        raise FileNotFoundError(f'No error files found under {folder}')

    # compile data
    data = []
    for f_name in f_names:
        logger.info(f'Bulk indexing file {f_name} to Elasticsearch...')
        with open(f_name, 'r') as f:
            for line in f:
                data.append(json.loads(line))

    # bulk indexing
    num_docs = len(data)
    logger.info(f'Bulk-indexing {num_docs:,} documents to Elasticsearch...')
    batch_size = 1000
    for i in range(0, num_docs, batch_size):
        try:
            es_client.bulk_index(data[i:(i+batch_size)])
        except Exception as e:
            logger.error(f'Failed to index batch {i}')
            raise e

if __name__ == "__main__":
    main()
