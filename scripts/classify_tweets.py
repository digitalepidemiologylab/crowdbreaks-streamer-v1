"""
This script pulls unclassified tweets from Elasticsearch and classifies them using sentence embeddings and an SVM model.
Run this script from withing <PROJECT_ROOT>/scripts 
Make sure to set all global vars first!
"""

import sys 
sys.path.append('..')
from multiprocessing import Pool, current_process
from logger import Logger
from elastic_search.elastic import Elastic
from elasticsearch import helpers
import requests
import instance.config
import json


def count_unlabelled():
    """Counts labelled tweets in index"""
    body_all = {'query': {'match_all': {}}}
    body_exists = { 'query': {'exists': {'field': 'meta.sentiment.{}'.format(MODEL)}}}
    body_not_exists = { 'query': {'bool': {'must_not': {'exists': {'field': 'meta.sentiment.{}'.format(MODEL)}}}}}
    count_all = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_all)['count'] 
    count_labelled = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_exists)['count'] 
    count_unlabelled = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=body_not_exists)['count'] 
    logger.info('Index {} contains a total of {} records of which {} are labelled and {} are unlabelled with model {}'\
            .format(INDEX, count_all, count_labelled, count_unlabelled, MODEL))
    return count_unlabelled


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
    logger.debug('Process {}: Processing tweet nr {}'.format(current_process().name, counter))

    # make sure doc does not contain sentiment
    model_exists = keys_exists(doc, '_source', 'meta', 'sentiment', MODEL)
    assert(not model_exists)

    # make sure text exists
    if not keys_exists(doc, '_source', 'text'):
        logger.error('Tweet {} contains no text field'.format(doc['_source']['id']))
        return

    # Make request 
    post_url = '{}/sentiment/vaccine'.format(instance.config.FLASK_API_HOSTNAME)
    resp = requests.post(post_url, json={'text': doc['_source']['text']}, auth=(instance.config.FLASK_API_USERNAME, instance.config.FLASK_API_PASSWORD))
    if resp.status_code == 200:
        resp = json.loads(resp.text)
        logger.debug("Predicted label of tweet {} as {}".format(doc['_source']['id'], resp['label']))
    else:
        logger.error("Something went wrong when predicting tweet {}".format(doc['_source']['id']))
        logger.error(resp.text)
        return

    # Update doc
    body = {'doc': {'meta': {'sentiment': {MODEL: resp}}}}
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
    """Label unlabelled tweets """
    body = {'query': {'bool': {'must_not': {'exists': {'field': 'meta.sentiment.{}'.format(MODEL)}}}}, 
            '_source': ['meta.sentiment.{}'.format(MODEL), 'id', 'text'] }
    estimated_time = "{}m".format(num_docs) # should give plenty of time for process to finish
    scan = helpers.scan(es_client.es, scroll=estimated_time, query=body, index=INDEX)
    count = 0
    for doc in scan:
        count += 1
        logger.debug('Processing doc {}/{}'.format(count, num_docs))
        # POOL.apply(process_document, args=(doc, count))
        process_document(doc, count)


def test_connection_flask_api():
    r = requests.get(instance.config.FLASK_API_HOSTNAME, auth=(instance.config.FLASK_API_USERNAME, instance.config.FLASK_API_PASSWORD))
    if r.status_code == 200:
        logger.info('Successfully connected to {}'.format(instance.config.FLASK_API_HOSTNAME))
    else:
        logger.error('Could not connect to {}'.format(instance.config.FLASK_API_HOSTNAME))

def main():
    # Test connection
    test_connection_flask_api()

    # Count unlabelled tweets in db
    num_docs = count_unlabelled()

    if num_docs == 0:
        logger.info('No work available... exiting')
        sys.exit()

    # Run script 
    yes_no = input('Would you like to label all unlabelled tweets (n={}) (y/n)?'.format(num_docs))
    if yes_no == 'y':
        logger.info('Start labelling tweets...')
        run(num_docs)
    else:
        logger.info('OK good night!')


if __name__ == "__main__":
    # logging
    logger = Logger.setup('script')
    logger.info('Starting classify tweets script...')

    # initialize ES
    es_client = Elastic()

    # global vars
    INDEX = 'project_vaccine_sentiment'
    DOC_TYPE='tweet'
    MODEL='sent2vec_v1.0'
    # NUM_CPUS=4
    # POOL = Pool(processes=NUM_CPUS)
    main()
