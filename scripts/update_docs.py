"""
This script bulk updates a field in Elasticsearch
"""
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError
import argparse
import logging
import pickle
from utils import get_es_client
from tqdm import tqdm
import json
from collections import defaultdict


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

def load_data(input_file):
    if input_file.endswith('.pkl'):
        with open(input_file, 'rb') as f:
            actions = pickle.load(f)
    elif input_file.endswith('.jsonl'):
        num_actions = sum(1 for _ in open(input_file, 'r'))
        with open(input_file, 'r') as f:
            for line in tqdm(f, total=num_actions):
                yield json.loads(line)

def main(args):
    es = get_es_client(env=args.es)
    logger.info(f'Streaming bulk operations from {args.input}...')
    resp = helpers.streaming_bulk(es.es, load_data(args.input), chunk_size=1000, max_retries=5, raise_on_error=not args.ignore_errors)
    for ok, _resp in resp:
        if not ok:
            logger.error(_resp)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, type=str, help='Name of of input file (pickled list of update actions)')
    parser.add_argument('--es', required=True, choices=['dev', 'stg', 'prd'], type=str, help='Which Elasticsearch instance to update docs on')
    parser.add_argument('--ignore-errors', dest='ignore_errors', action='store_true', help='Ignore errors which happen during indexing')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
