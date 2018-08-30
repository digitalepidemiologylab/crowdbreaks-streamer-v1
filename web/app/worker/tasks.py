from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
import time
import logging
from copy import copy
from app.utils.process_tweet import ProcessTweet
from app.utils.priority_queue import TweetIdQueue
from app.extensions import es
import fastText
import json
import os


@celery.task(ignore_result=True)
def process_tweet(tweet, send_to_es=True, use_pq=True, debug=True):
    """Process incoming tweets

    :param tweet: JSON tweet object
    """
    # logging
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)

    # Todo: incorporate filter function

    # Strip from certain fields
    pt = ProcessTweet(tweet)
    pt.strip()

    # compute average location from bounding box (reducing storage on ES)
    if pt.tweet['place'] is not None and pt.tweet['place']['bounding_box'] is not None:
        pt.compute_average_location()
        logger.debug('Computed average location {} and average radius {}'.format(pt.processed_tweet['place']['average_location'], 
            pt.processed_tweet['place']['location_radius']))

    # Add tweet into redis-based priority queue (for users to classify)
    if use_pq:
        logger.debug('Add tweet to priority queue...')
        tid = TweetIdQueue(pt.tweet['project'], priority_threshold=3)
        tid.add(pt.tweet['id'], priority=0)

    # If tweet belongs to vaccine sentiment project, tokenize and classify
    if pt.tweet['project'] == 'project_vaccine_sentiment':
        logger.debug('About to tokenize...')
        text_tokenized = pt.tokenize()
        logger.debug('Text "{}" was tokenized to "{}"'.format(pt.tweet['text'], pt.processed_tweet['text']))
        if text_tokenized is not None:
            # classify tweet
            model = 'fasttext_v1.ftz'
            prediction = predict(text_tokenized, model=model)
            meta = {'sentiment': {str(model.split('.')[0]): {'label': prediction['labels'][0], 'label_val': prediction['label_vals'][0], 'probability': prediction['probabilities'][0]}}}
            logger.debug('meta: {}'.format(meta))
            pt.add_meta(meta)
        else:
            logger.debug("Text {} is either too short or could not be properly tokenized.".format(text_tokenized))
    processed_tweet = pt.get_processed_tweet() 
    logger.debug('Processed tweet:\n{}'.format(processed_tweet))

    if send_to_es:
        logger.debug('Sending tweet with id {} to ES'.format(processed_tweet['id']))
        index_tweet_es(processed_tweet)

def index_tweet_es(tweet):
    logger = get_task_logger(__name__)
    logger.debug("Indexing tweet with id {} to ES".format(tweet['id']))
    es.index_tweet(tweet)
