from multiprocessing import Pool, current_process
import redis
import json
import config
import app
from process_tweet import ProcessTweet
import time
from copy import copy
import pdb
from logger import Logger
import pickle as pkl
import os, sys
import numpy as np
import uuid

def process_from_logstash(tweet):
    # Note to future self: sharing connections like that might be problematic,
    # check: https://stackoverflow.com/questions/28638939/python3-x-how-to-share-a-database-connection-between-processes 
    redis_conn = redis.Redis(connection_pool=app.POOL)

    # Strip json
    tweet_stripped = ProcessTweet.strip(copy(tweet))

    # Todo: incorporate filter function

    # compute average location from bounding box (reducing storage on ES)
    if tweet['place'] is not None and tweet['place']['bounding_box'] is not None:
        tweet_stripped = ProcessTweet.compute_average_location(tweet, tweet_stripped)

    # If tweet belongs to vaccine sentiment project, tokenize and compute sentence embeddings
    if tweet_stripped['project'] == 'vaccine_sentiment':
        text_tokenized = ProcessTweet.tokenize(copy(tweet_stripped['text']))
        if text_tokenized is None:
            logger.debug("Field text is either too short or could not be properly tokenized.")
            redis_conn.rpush(submit_queue, json.dumps(tweet_stripped))
        else:
            tweet_stripped['text_tokenized'] = text_tokenized.strip()
            tweet_stripped['result_queue'] = embedding_output_queue
            logger.debug("Sending tweet with id {} to {} queue".format(tweet_stripped['id'], embedding_input_queue))
            redis_conn.rpush(embedding_input_queue, json.dumps(tweet_stripped))
    else:
        # Else push to ES submit queue
        logger.debug('Process {}: Pushing tweet from project {} (id: {}) to submit queue'.format(current_process().name, tweet_stripped['project'], tweet_stripped['id']))
        redis_conn.rpush(submit_queue, json.dumps(tweet_stripped))


def submit_tweet(tweet):
    logger.debug("Indexing tweet with id {} to ES".format(tweet['id']))
    app.es.index_tweet(tweet)


def compute_sentiment(tweet_obj, model='sent2vec_v1.0'):
    """Classify sentiment of vaccine sentiment related project
    :param tweet_obj: Tweet object to classify 
    """
    redis_conn = redis.Redis(connection_pool=app.POOL)
    logger.debug("Compute sentiment for tweet_obj with id {}".format(tweet_obj['id']))
    if 'sentence_vector' not in tweet_obj:
        logger.error("Tweet with id {} has no field 'sentence_vector'".format(tweet_obj['id']))
    elif not isinstance(tweet_obj['sentence_vector'], list) or len(tweet_obj['sentence_vector']) < 1:
        logger.error("Tweet with id {} has an invalid sentence_vector".format(tweet_obj['id']))

    # Run classifier
    label, distances = classify(tweet_obj['sentence_vector'])

    # Clean up
    delete_keys = ['sentence_vector', 'text_tokenized', 'result_queue']
    for del_key in delete_keys:
        if del_key in tweet_obj:
            del tweet_obj[del_key]

    # add meta information
    if label is not None:
        meta = {'sentiment': {str(model): {'label': label, 'distances': distances}}}
    else:
        meta = {'sentiment': {str(model): {'label': 'not determinable'}}}

    tweet_obj['meta'] = meta
    logger.debug("Tweet with id {} predicted to be of label '{}' ".format(tweet_obj['id'], label))

    # Send to submit queue
    redis_conn.rpush(submit_queue, json.dumps(tweet_obj))


def classify(sentence_vector, model='sent2vec_v1.0'):
    """Compute label based on sentence vector

    :param sentence_vector: In case of sent2vec represents 1x700 dimensional vector to be classified
    :param model: filename of classifier to be used
    :returns: predicted label, distances to separating hyperplane
    """
    input_vec = np.zeros([1, len(sentence_vector)])
    input_vec[0] = np.asarray(sentence_vector)
    # Load classifier into memory of process
    f_clf = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bin', 'vaccine_sentiment', model + '.p')
    with open(f_clf, 'rb') as f:
        clf = pkl.load(f)
    try: 
        label = clf.predict(input_vec)[0]
        distances = clf.decision_function(input_vec)[0]
    except Exception as e:
        logger.error(e)
        return None, None
    else:
        label_dict = {-1: 'anti-vaccine', 0:'neutral', 1:'pro-vaccine'}
        return label_dict[label], list(distances)


def vaccine_sentiment_single_request(input_data, logger):
    """Handle single request from Flask
    :param input_data: Text object containing a field 'text'
    :returns: Classified label
    """
    redis_conn = redis.Redis(connection_pool=app.POOL)
    q_name = queue_name('single_request_{}'.format(uuid.uuid4()))
    text_tokenized = ProcessTweet.tokenize(copy(input_data['text']))
    if text_tokenized is None:
        return 'undeterminable', None

    input_data['text_tokenized'] = text_tokenized.strip()
    input_data['result_queue'] = q_name
    input_data['mode'] = 'single_request'
    embedding_input_queue = queue_name(config.REDIS_EMBEDDING_INPUT_QUEUE_KEY)
    redis_conn.rpush(embedding_input_queue, json.dumps(input_data))

    # wait for result
    _, _res = redis_conn.blpop([q_name])
    res = json.loads(_res)

    if 'sentence_vector' not in res or not isinstance(res['sentence_vector'], list) or len(res['sentence_vector']) < 1:
        return 'undeterminable', None
    else:
        return classify(res['sentence_vector'])


def queue_name(name):
    return "{}:{}".format(config.REDIS_NAMESPACE, name)


def main(parallel=True):
    """main

    :param parallel:
    :param with_sleep:
    """

    while True:
        logger.debug('Fetching new work...')
        redis_conn = redis.Redis(connection_pool=app.POOL)

        # Pop from queues and assign job to a free worker...
        _q, _tweet = redis_conn.blpop([logstash_queue, submit_queue, embedding_output_queue])
        tweet = json.loads(_tweet)
        q_name = _q.decode()
        if parallel:
            if q_name == logstash_queue:
                res = preprocess_pool.apply_async(process_from_logstash, args=(tweet,))
            elif q_name == submit_queue:
                res = submit_pool.apply_async(submit_tweet, args=(tweet,))
            elif q_name == embedding_output_queue:
                res = embedding_pool.apply_async(compute_sentiment, args=(tweet,))
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))
        else:
            # For debug purposes... (will be deleted)
            if q_name == logstash_queue:
                process_from_logstash(tweet)
            elif q_name == submit_queue:
                submit_tweet(tweet)
            elif q_name == embedding_output_queue:
                compute_sentiment(tweet)
            else:
                logger.warning("Queue name {} is not being processed".format(q_name))


if __name__ == '__main__':
    PARALLEL = False

    # set up logging
    logger = Logger.setup('worker', filename='worker.log')
    logger.info('Hello from worker logger!')

    # Check for classifier file
    clf_file = 'sent2vec_v1.0'
    f_clf = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bin', 'vaccine_sentiment', clf_file + '.p')
    if not os.path.isfile(f_clf):
        logger.error('File under {} could not be found.'.format(f_clf))
        sys.exit()

    # Queue for tweets coming from logstash
    logstash_queue = queue_name(config.REDIS_LOGSTASH_QUEUE_KEY)

    # Queue to push tweets into which can be sent to ElasticSearch
    submit_queue = queue_name(config.REDIS_SUBMIT_QUEUE_KEY)

    # Queue sent2vec listens to
    embedding_input_queue = queue_name(config.REDIS_EMBEDDING_INPUT_QUEUE_KEY)

    # queue to for sent2vec to push tweet results into
    embedding_output_queue = queue_name(config.REDIS_EMBEDDING_OUTPUT_QUEUE_KEY)

    # Process pools
    if PARALLEL:
        logger.info("Starting worker pools...")
        preprocess_pool = Pool(processes=config.NUM_PROCESSES_PREPROCESSING)
        submit_pool = Pool(processes=config.NUM_SUBMIT_PREPROCESSING)
        embedding_pool = Pool(processes=config.NUM_EMBEDDING_PREPROCESSING)

    main(parallel=PARALLEL)
