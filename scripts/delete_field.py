"""Delete arbitrary field from all documents which contain said field in index"""

import sys 
sys.path.append('..')
from multiprocessing import Pool, current_process
from logger import Logger
from connections import elastic
from elasticsearch import helpers
import requests
import instance.config
import json
import redis
import worker


def count_docs_with_field(field):
    """counts documents with given field in index"""
    body_all = {'query': {'match_all': {}}}
    body_exists = { 'query': {'exists': {'field': field}}}
    body_not_exists = { 'query': {'bool': {'must_not': {'exists': {'field': field}}}}}
    count_all = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_all)['count'] 
    count_with = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_exists)['count'] 
    count_without = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_not_exists)['count'] 
    return count_all, count_with, count_without


def keys_exists(element, *keys):
    """ Check if *keys (nested) exists in `element` (dict). """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True


def run(num_docs):
    """run

    :param num_docs:
    """
    body = {'query': {'exists': {'field': FIELD}}, 
            '_source': [FIELD, 'id', 'text'] }
    estimated_time = "{}m".format(num_docs) # should give plenty of time for process to finish
    scan = helpers.scan(es_client.es, scroll=estimated_time, query=body, index=INDEX)
    count = 0
    for doc in scan:
        count += 1
        logger.debug('Processing doc {}/{}'.format(count, num_docs))
        res = es_client.delete_field_from_doc(INDEX, DOC_TYPE, doc['_id'], MODEL, field_path=FIELD_PATH)
        logger.debug("Response: {}".format(res))

def test_connection_flask_api():
    r = requests.get(instance.config.FLASK_API_HOSTNAME, auth=(instance.config.FLASK_API_USERNAME, instance.config.FLASK_API_PASSWORD))
    if r.status_code == 200:
        logger.info('Successfully connected to {}'.format(instance.config.FLASK_API_HOSTNAME))
    else:
        logger.error('Could not connect to {}'.format(instance.config.FLASK_API_HOSTNAME))

def main():
    # Count unlabelled tweets in db
    count_all, count_with, count_without = count_docs_with_field(FIELD)

    if count_with == 0:
        logger.info('No work available... exiting')
        sys.exit()

    logger.info('Found {} documents with field {} in index with a total of {} documents'.format(count_with, FIELD, count_all))

    # Run script 
    yes_no = input('Would you like to delete the field {} in {} tweets (y/n)?'.format(MODEL, count_with))
    if yes_no == 'y':
        logger.info('Start deleting ...')
        run(count_with)
    else:
        logger.info('OK good night!')

if __name__ == "__main__":
    # logging
    logger = Logger.setup('script')
    logger.info('Starting deletion script...')

    # initialize ES
    es_client = elastic.Elastic()

    # global vars
    INDEX = 'project_vaccine_sentiment'
    DOC_TYPE='tweet'
    MODEL='sent2vec_v1'
    FIELD_PATH = 'meta.sentiment' 
    FIELD = '{}.{}'.format(FIELD_PATH, MODEL)
    main()
