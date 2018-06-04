"""
This script updates a field
Run this script from within <PROJECT_ROOT>/scripts 
Make sure to set all global vars first!

This script has to be run on a server with access to Elasticsearch!
"""

import sys 
sys.path.append('..')
sys.path.append('../web/')
from multiprocessing import Pool, current_process
from web.app.connections import elastic
from elasticsearch import helpers
import requests
import json
import logging.config
from web.app.worker.tasks import predict
from web.app.worker.process_tweet import ProcessTweet


def count_to_be_updated():
    """counts labelled tweets in index"""
    
    # label should exist on all considered records
    default_exist = {'exists': {'field': 'meta.sentiment.{}.label'.format(MODEL)}}
    body_all = {'query': default_exist}
    body_exists = {'query': {'bool': {'must': [{'exists': {'field': 'meta.sentiment.{}.label_val'.format(MODEL)}}, default_exist]}}}
    body_not_exists = {'query': {'bool': {'must_not': {'exists': {'field': 'meta.sentiment.{}.label_val'.format(MODEL)}}, 'must': default_exist}}}
    count_all = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_all)['count'] 
    count_updated = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_exists)['count'] 
    count_not_updated = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_not_exists)['count'] 
    logger.info('index {} contains a total of {} records of which {} are already updated and {} are not updated'\
            .format(INDEX, count_all, count_updated, count_not_updated))
    return count_not_updated


def keys_exists(element, *keys):
    """ Check if *keys (nested) exists in `element` (dict). """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

def process_document(doc, counter):
    """Single worker task"""
    # Update doc
    print(doc)
    body = {'doc': {'meta': {'sentiment': {MODEL: {'label_val': LABEL_DICT[doc['_source']['meta']['sentiment'][MODEL]['label']]}}}}}
    resp = es_client.es.update(index=INDEX, doc_type=DOC_TYPE, id=doc['_id'], body=body)
    if resp['_shards']['failed'] == 0:
        logger.debug("Successfully updated document {}.".format(doc['_source']['id']))
    else:
        logger.error("Update of document {} failed".format(doc['_source']['id']))
    return


def run(num_docs):
    """run

    :param num_docs:
    """
    default_exist = {'exists': {'field': 'meta.sentiment.{}.label'.format(MODEL)}}
    body_not_exists = {'bool': {'must_not': {'exists': {'field': 'meta.sentiment.{}.label_val'.format(MODEL)}}, 'must': default_exist}}
    body = { 'query': body_not_exists, '_source': ['meta.sentiment.{}.label'.format(MODEL), 'id'] }
    estimated_time = "{}m".format(num_docs) # should give plenty of time for process to finish
    scan = helpers.scan(es_client.es, scroll=estimated_time, query=body, index=INDEX)
    count = 0
    for doc in scan:
        count += 1
        logger.debug('Processing doc {}/{}'.format(count, num_docs))
        process_document(doc, count)


def main():
    # Count unlabelled tweets in db
    num_docs = count_to_be_updated()

    if num_docs == 0:
        logger.info('No work available... exiting')
        sys.exit()

    # Run script 
    yes_no = input('Would you like to update all tweets (n={}) (y/n)?'.format(num_docs))
    if yes_no == 'y':
        logger.info('Start labelling tweets...')
        run(num_docs)
    else:
        logger.info('OK good night!')


if __name__ == "__main__":
    # logging
    logging.config.fileConfig('script_logging.conf')
    logger = logging.getLogger('script')
    logger.info('Starting classify tweets script...')

    # initialize ES
    es_client = elastic.Elastic()
    if not es_client.test_connection():
        logger.error('Could not connect to Elasticsearch. Exiting.')

    # global vars
    INDEX = 'project_vaccine_sentiment'
    DOC_TYPE='tweet'
    MODEL='fasttext_v1'
    LABEL_DICT = {'pro-vaccine': 1, 'anti-vaccine':-1, 'neutral': 0}
    main()
