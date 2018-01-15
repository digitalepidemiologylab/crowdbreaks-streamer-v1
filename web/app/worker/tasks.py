from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
import time
import logging
from copy import copy
from app.worker.process_tweet import ProcessTweet
from app.worker.priority_queue import TweetIdQueue
from app.connections import elastic
import json


# logging
logger = get_task_logger(__name__)


@celery.task(name='cb.process_tweet', ignore_result=True)
def process_tweet(tweet):
    """Process incoming tweets (currently task is triggered by logstash)

    :param tweet: JSON tweet object
    """
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
    tid = TweetIdQueue(pt.tweet['project'], priority_threshold=3)
    tid.add(pt.tweet['id'], priority=0)

    # If tweet belongs to vaccine sentiment project, tokenize and classify
    if pt.tweet['project'] == 'project_vaccine_sentiment':
        text_tokenized = pt.tokenize(pt.tweet['text'])
        if text_tokenized is not None:
            processed_tweet = pt.get_processed_tweet() 
            logger.info('Tweet will be classified')
            # classify tweet
        else:
            logger.debug("Text {} is either too short or could not be properly tokenized.".format(text_tokenized))
        index_tweet_es(pt.get_processed_tweet())
    else:
        processed_tweet = pt.get_processed_tweet() 
        logger.debug('Pushing tweet from project {} (id: {}) to submit queue'.format(processed_tweet['project'], processed_tweet['id']))
        index_tweet_es(processed_tweet)


def index_tweet_es(tweet):
    es = elastic.Elastic()
    logger.debug("Indexing tweet with id {} to ES".format(tweet['id']))
    es.index_tweet(tweet)
    return
    
