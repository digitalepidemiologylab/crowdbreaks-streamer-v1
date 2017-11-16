from multiprocessing import Pool, current_process
import redis
import json
from app import app, es, POOL
from process_tweet import ProcessTweet
import time
from copy import copy
import pdb
from logger import Logger


def process_from_logstash(tweet):
    # Note to future self: sharing connections like that might be problematic,
    # check: https://stackoverflow.com/questions/28638939/python3-x-how-to-share-a-database-connection-between-processes 
    redis_conn = redis.Redis(connection_pool=POOL)

    # Strip json
    tweet_stripped = ProcessTweet.strip(copy(tweet))

    # Todo: incorporate filter function

    # compute average location from bounding box (reducing storage on ES)
    if tweet['place'] is not None and tweet['place']['bounding_box'] is not None:
        tweet_stripped = ProcessTweet.compute_average_location(tweet, tweet_stripped)

    if tweet_stripped['project'] == 'vaccine_sentiment':
        # If tweet belongs to vaccine sentiment project, compute sentiment
        logger.debug('Process {}: Pushing tweet from project {} (id: {}) to sentiment queue'.format(current_process().name, tweet['project'], tweet['id']))
        redis_conn.rpush(sentiment_queue, json.dumps(tweet_stripped))
    else:
        # Else push to ES submit queue
        logger.debug('Process {}: Pushing tweet from project {} (id: {}) to submit queue'.format(current_process().name, tweet['project'], tweet['id']))
        redis_conn.rpush(submit_queue, json.dumps(tweet_stripped))


def queue_name(name):
    return "{}:{}".format(app.config['REDIS_NAMESPACE'], name)


def submit_tweet(tweet):
    logger.debug("Indexing tweet with id {} to ES".format(tweet['id']))
    es.index_tweet(tweet)


def compute_sentiment(tweet):
    logger.debug("Compute sentiment for tweet with id {} to ES".format(tweet['id']))


def main(parallel=True):
    """main

    :param parallel:
    :param with_sleep:
    """

    while True:
        logger.debug('Fetching new work...')
        redis_conn = redis.Redis(connection_pool=POOL)

        # Pop from queues and assign job to a free worker...
        _q, _tweet = redis_conn.blpop([logstash_queue, submit_queue, sentiment_queue])
        tweet = json.loads(_tweet)
        q_name = _q.decode()
        if parallel:
            if q_name == logstash_queue:
                res = preprocess_pool.apply_async(process_from_logstash, args=(tweet,))
            elif q_name == submit_queue:
                res = submit_pool.apply_async(submit_tweet, args=(tweet,))
            elif q_name == sentiment_queue:
                res = sentiment_pool.apply_async(compute_sentiment, args=(tweet,))
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
        else:
            if q_name == logstash_queue:
                process_tweet(tweet)
            elif q_name == submit_queue:
                submit_tweet(tweet)
            elif q_name == sentiment_queue:
                compute_sentiment(tweet)
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
                
            logger.info('That was a lot of work... sleeping for a bit now')
            time.sleep(0.2)


if __name__ == '__main__':
    # set up logging
    logger = Logger.setup('worker', filename='worker.log')

    # queue names
    logstash_queue = queue_name(app.config['REDIS_LOGSTASH_QUEUE_KEY'])
    submit_queue = queue_name(app.config['REDIS_SUBMIT_QUEUE_KEY'])
    sentiment_queue = queue_name(app.config['REDIS_SENTIMENT_QUEUE_KEY'])

    # Process pools
    logger.info("Starting worker pools...")
    preprocess_pool = Pool(processes=app.config['NUM_PROCESSES_PREPROCESSING'])
    submit_pool = Pool(processes=app.config['NUM_SUBMIT_PREPROCESSING'])
    sentiment_pool = Pool(processes=app.config['NUM_SENTIMENT_PREPROCESSING'])

    main(parallel=True)
