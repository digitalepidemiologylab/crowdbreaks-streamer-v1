"""
This script bulk updates a field in Elasticsearch
"""
from elasticsearch import helpers
import argparse
import logging
import pickle
from utils import get_es_client
from tqdm import tqdm


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

def load_data(input_file):
    with open(input_file, 'rb') as f:
        actions = pickle.load(f)
    return actions

def main(args):
    es = get_es_client(env=args.es)
    logger.info(f'Loading data from {args.input}...')
    actions = load_data(args.input)
    num_actions = len(actions)
    logger.info(f'Loaded {num_actions:,} actions...')
    logger.info('Start updating docs...')
    batch_size = 1000
    for i in tqdm(range(0, num_actions, batch_size)):
        try:
            resp = helpers.bulk(es.es, actions[i:(i+batch_size)])
        except Exception as e:
            logger.error('Bulk updating not unsuccessful!')
            print(e)
        else:
            logger.info(f'Successfully indexed {resp[0]:,} batches of docs...')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, type=str, help='Name of of input file (pickled list of update actions)')
    parser.add_argument('--es', required=True, choices=['dev', 'stg', 'prd'], type=str, help='Which Elasticsearch instance to update docs on')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
