from multiprocessing import Pool, current_process
import redis
import json
from app import app, es, POOL
from process_tweet import ProcessTweet
import time
from copy import copy
import pdb
from logger import Logger
import pickle as pkl
import os, sys
import numpy as np

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

def compute_sentiment(embedded_text_obj):
    """Classify sentiment based on word embeddings given in 'sentence_vector'. If part of 'vaccine_sentiment' project push tweet into ES, otherwise return predicted label
    :param embedding_text_obj: Text object to classify 
    :returns: None if part of vaccine_sentiment_tracking project, otherwise returns classified label and distances to hyperplane 
    """
    redis_conn = redis.Redis(connection_pool=POOL)
    logger.debug("Compute sentiment for embedded_text_obj with id {}".format(embedded_text_obj['id']))
    labelled_successfully = False
    label = None
    if 'sentence_vector' not in embedded_text_obj:
        logger.error("Tweet with id {} has no field 'sentence_vector'".format(embedded_text_obj['id']))
    elif not isinstance(embedded_text_obj['sentence_vector'], list) or len(embedded_text_obj['sentence_vector']) < 1:
        logger.error("Tweet with id {} has an invalid sentence_vector".format(embedded_text_obj['id']))
    else:
        # Run classifier
        input_vec = np.zeros([1, len(embedded_text_obj['sentence_vector'])])
        input_vec[0] = embedded_text_obj['sentence_vector']

        # Load classifier into memory of process
        f_clf = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bin', 'vaccine_sentiment', clf_file + '.p')
        with open(f_clf, 'rb') as f:
            clf = pkl.load(f)
        try: 
            label = clf.predict(input_vec)[0]
            distances = clf.decision_function(input_vec)[0]
        except Exception as e:
            logger.error(e)
            logger.error("Error occured when trying to predict tweet {}".format(embedded_text_obj['id']))
        else:
            labelled_successfully = True

    # Clean up
    delete_keys = ['sentence_vector', 'text_tokenized']
    for del_key in delete_keys:
        if del_key in embedded_text_obj:
            del embedded_text_obj[del_key]

    if 'project' in embedded_text_obj and embedded_text_obj['project'] == 'vaccine_sentiment':
        if labelled_successfully:
            # add meta information
            label_dict = {-1: 'anti-vaccine', 0:'other', 1:'pro-vaccine'}
            meta = {'sentiment': {clf_file: {'label': label_dict[label], 'distances': list(distances)}}}
            embedded_text_obj['meta'] = meta
            logger.debug("Tweet with id {} predicted to be of label '{}' ".format(embedded_text_obj['id'], label_dict[label]))

        # Send to submit queue
        redis_conn.rpush(submit_queue, json.dumps(embedded_text_obj))
    else:
        return label



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
            # time.sleep(0.1)
        else:
            # For debug purposes... (will be deleted)
            if q_name == logstash_queue:
                process_from_logstash(tweet)
            elif q_name == submit_queue:
                submit_tweet(tweet)
            elif q_name == embedding_result_queue:
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

    # load classifier
    clf_file = 'sent2vec_v1.0'
    f_clf = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'bin', 'vaccine_sentiment', clf_file + '.p')
    try:
        f = open(f_clf, 'rb')
    except IOError:
        logger.error('File under {} could not be found or opened.'.format(f_clf))
        sys.exit()
    f.close()

    main(parallel=True)
