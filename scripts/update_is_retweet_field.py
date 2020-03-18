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
from web.app.utils.process_tweet import ProcessTweet
import re


def count_to_be_updated():
    """counts labelled tweets in index"""
    count = es_client.es.count(index=INDEX, doc_type=DOC_TYPE, body=get_query())['count']
    logger.info('index {} contains a total of {} records which need to be updated...'.format(INDEX, count))
    return count

def process_document(doc, counter):
    """Single worker task"""
    # Update doc
    text = doc['_source']['text']
    is_retweet = False
    if re.match(r'^RT @\w+', text) is not None:
        is_retweet = True
    body = {'doc': {'is_retweet': is_retweet}}
    resp = es_client.es.update(index=INDEX, doc_type=DOC_TYPE, id=doc['_id'], body=body)
    if resp['_shards']['failed'] == 0:
        logger.debug("Successfully updated document {}.".format(doc['_id']))
    else:
        logger.error("Update of document {} failed".format(doc['_id']))
    return

def get_query():
    query = {
            'query': {
                'bool': {
                    'must_not': [
                        {
                            'exists': {
                                'field': 'is_retweet'
                                }
                            }
                        ],
                    'must': [
                        {
                            'range': {
                                'created_at': {
                                    "gte": "Mon Sep 03 09:49:33 +0000 2018",
                                    "lte": "now"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
    return query


def run():
    """run

    :param num_docs:
    """
    body = { **get_query(), '_source': ['text']}
    estimated_time = "{}m".format(24*60) # should give plenty of time for process to finish
    scan = helpers.scan(es_client.es, scroll=estimated_time, query=body, index=INDEX)
    count = 0
    num_docs = count_to_be_updated()
    for doc in scan:
        count += 1
        logger.debug('Processing doc {}/{}'.format(count, num_docs))
        process_document(doc, count)

if __name__ == "__main__":
    # logging
    logging.config.fileConfig('script_logging.conf')
    logger = logging.getLogger('script')
    logger.info('Starting classify tweets script...')

    es_client = elastic.Elastic()
    if not es_client.test_connection():
        logger.error('Could not connect to Elasticsearch. Exiting.')

    # global vars
    INDEX = 'project_vaccine_sentiment'
    DOC_TYPE='tweet'

    # Count unlabelled tweets in db
    num_docs = count_to_be_updated()

    if num_docs == 0:
        logger.info('No work available... exiting')
        sys.exit()

    # Run script
    yes_no = input('Would you like to update all tweets (n={}) (y/n)?'.format(num_docs))
    if not yes_no == 'y':
        logger.info('OK good night!')
        sys.exit()

    # initialize ES
    num_connection_retrails = 5
    for i in range(num_connection_retrails):
        try:
            es_client = elastic.Elastic()
            run()
            logger.info('... done!')
        except Exception as e:
            print(e.message)
            pass
