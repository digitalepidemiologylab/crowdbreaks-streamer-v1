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

    # Strip json
    tweet_stripped = ProcessTweet.strip(copy(tweet))

    # Todo: incorporate filter function

    # compute average location from bounding box (reducing storage on ES)
    if tweet['place'] is not None and tweet['place']['bounding_box'] is not None:
        tweet_stripped = ProcessTweet.compute_average_location(tweet, tweet_stripped)

    if tweet_stripped['project'] == 'vaccine_sentiment':
        # If tweet belongs to vaccine sentiment project, compute sentence embeddings
        compute_embedding(tweet_stripped)
    else:
        # Else push to ES submit queue
        redis_conn = redis.Redis(connection_pool=POOL)
        logger.debug('Process {}: Pushing tweet from project {} (id: {}) to submit queue'.format(current_process().name, tweet['project'], tweet['id']))
        redis_conn.rpush(submit_queue, json.dumps(tweet_stripped))


def compute_embedding(input_text_obj):
    """Tokenize and send to embedding queue. Based on 'text' field a new field 'text_tokenized' is computed, containing the tokenized field. 
    :param input_text_obj: Input dictionary object, needs to contain at least an id and text field.
    """
    redis_conn = redis.Redis(connection_pool=POOL)
    text_not_available = 'text' not in input_text_obj or input_text_obj['text'] == ""
    id_not_available = 'id' not in input_text_obj
    if text_not_available or id_not_available or not isinstance(input_text_obj, dict):
        logger.error('Object contains no text or no id or is not of type dict.')
        return
    input_text_tokenized = ProcessTweet.tokenize(copy(input_text_obj['text']))
    if input_text_tokenized is None:
        logger.warning('Input_text_obj with id {} and text {} could not be tokenized.'.format(input_text_obj['id'], input_text_obj['text']))
        return
    input_text_obj['text_tokenized'] = input_text_tokenized
    logger.debug('Process {}: Pushing input_text_obj with id: {} to embedding queue'.format(current_process().name, input_text_obj['id']))
    redis_conn.rpush(embedding_queue, json.dumps(input_text_obj))


def queue_name(name):
    return "{}:{}".format(app.config['REDIS_NAMESPACE'], name)

def submit_tweet(tweet):
    logger.debug("Indexing tweet with id {} to ES".format(tweet['id']))
    es.index_tweet(tweet)

def compute_sentiment(tweet):
    logger.debug("Compute sentiment for tweet with id {} to ES".format(tweet['id']))

    # Run classifier
    # Clean up tweet

    # Send to submit queue
    redis_conn.rpush(submit_queue, json.dumps(tweet_stripped))


def main(parallel=True):
    """main

    :param parallel:
    :param with_sleep:
    """

    while True:
        logger.debug('Fetching new work...')
        redis_conn = redis.Redis(connection_pool=POOL)

        # Pop from queues and assign job to a free worker...
        _q, _tweet = redis_conn.blpop([logstash_queue, submit_queue, embedding_result_queue])
        tweet = json.loads(_tweet)
        q_name = _q.decode()
        if parallel:
            if q_name == logstash_queue:
                res = preprocess_pool.apply_async(process_from_logstash, args=(tweet,))
            elif q_name == submit_queue:
                res = submit_pool.apply_async(submit_tweet, args=(tweet,))
            elif q_name == embedding_result_queue:
                res = embedding_pool.apply_async(compute_sentiment, args=(tweet,))
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
        else:
            # For debug purposes... (will be deleted)
            if q_name == logstash_queue:
                process_from_logstash(tweet)
            elif q_name == submit_queue:
                submit_tweet(tweet)
            elif q_name == embedding_queue:
                compute_sentiment(tweet)
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
                
            logger.info('That was a lot of work... sleeping for a bit now')
            time.sleep(1)


if __name__ == '__main__':
    # set up logging
    logger = Logger.setup('worker', filename='worker.log')

    # queue names
    logstash_queue = queue_name(app.config['REDIS_LOGSTASH_QUEUE_KEY'])
    submit_queue = queue_name(app.config['REDIS_SUBMIT_QUEUE_KEY'])
    embedding_queue = queue_name(app.config['REDIS_EMBEDDING_QUEUE_KEY'])
    embedding_result_queue = queue_name(app.config['REDIS_EMBEDDING_RESULT_QUEUE_KEY'])

    # Process pools
    logger.info("Starting worker pools...")
    preprocess_pool = Pool(processes=app.config['NUM_PROCESSES_PREPROCESSING'])
    submit_pool = Pool(processes=app.config['NUM_SUBMIT_PREPROCESSING'])
    embedding_pool = Pool(processes=app.config['NUM_EMBEDDING_PREPROCESSING'])

    main(parallel=True)
