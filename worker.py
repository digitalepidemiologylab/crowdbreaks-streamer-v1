from multiprocessing import Pool
import multiprocessing
import redis
import json
from app import app, POOL, es
from process_tweet import ProcessTweet
import time
from copy import copy
import pdb
from logger import Logger


def process_tweet(tweet, redis_conn):
    current_process = multiprocessing.current_process()
    logger.debug("Process {} is now processing tweet with id {}".format(current_process.name, tweet['id_str']))

    # Strip json
    tweet_stripped = ProcessTweet.strip(copy(tweet))

    # Todo: incorporate filter function

    # compute average location from bounding box (reducing storage on ES)
    if tweet['place'] is not None and tweet['place']['bounding_box'] is not None:
        tweet_stripped = ProcessTweet.compute_average_location(tweet, tweet_stripped)

    if tweet_stripped['project'] is not None and tweet_stripped['project'] == 'vaccine_sentiment_tracking':
        # If tweet belongs to vaccine sentiment project, compute sentiment
        redis_conn.rpush(queue_name(app.config['REDIS_SENTIMENT_QUEUE_KEY']), json.dumps(tweet_stripped))
    else:
        # Else push to ES submit queue
        redis_conn.rpush(queue_name(app.config['REDIS_SUBMIT_QUEUE_KEY']), json.dumps(tweet_stripped))
    return

def queue_name(name):
    return "{}:{}".format(app.config['REDIS_NAMESPACE'], name)

def submit_tweet(tweet, redis_conn):
    logger.debug("Submitting tweet with id {} to ES".format(tweet['id']))

def compute_sentiment(tweet, redis_conn):
    logger.debug("Compute sentiment for tweet with id {} to ES".format(tweet['id']))

def main(parallel=True):
    """main

    :param parallel:
    :param with_sleep:
    """
    # queue names
    logstash_queue = queue_name(app.config['REDIS_LOGSTASH_QUEUE_KEY'])
    submit_queue = queue_name(app.config['REDIS_SUBMIT_QUEUE_KEY'])
    sentiment_queue = queue_name(app.config['REDIS_SENTIMENT_QUEUE_KEY'])

    logger.info("Starting worker pools...")
    logger.debug('Start logging...')
    preprocess_pool = Pool(processes=app.config['NUM_PROCESSES_PREPROCESSING'])
    submit_pool = Pool(processes=app.config['NUM_SUBMIT_PREPROCESSING'])
    sentiment_pool = Pool(processes=app.config['NUM_SENTIMENT_PREPROCESSING'])

    while True:
        logger.info('Fetching new work...')
        redis_conn = redis.Redis(connection_pool=POOL)
        # pop from queues and assign job to a free worker...
        # _q, _tweet = redis_conn.blpop([logstash_queue, submit_queue, sentiment_queue])
        _q, _tweet = redis_conn.blpop([sentiment_queue, submit_queue])
        tweet = json.loads(_tweet)
        q_name = _q.decode()
        if parallel:
            if q_name == logstash_queue:
                res = preprocess_pool.apply_async(process_tweet, args=(tweet, redis_conn))
            elif q_name == submit_queue:
                res = submit_pool.apply_async(submit_tweet, args=(tweet, redis_conn))
            elif q_name == sentiment_queue:
                res = sentiment_pool.apply_async(compute_sentiment, args=(tweet, redis_conn))
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
        else:
            if q_name == logstash_queue:
                process_tweet(tweet, redis_conn)
            elif q_name == submit_queue:
                submit_tweet(tweet, redis_conn)
            elif q_name == sentiment_queue:
                compute_sentiment(tweet, redis_conn)
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
                
            logger.info('That was a lot of work... sleeping for a bit now')
            time.sleep(0.2)


if __name__ == '__main__':
    # set up logging
    logger = Logger.setup('worker', filename='worker.log')

    main(parallel=False)
