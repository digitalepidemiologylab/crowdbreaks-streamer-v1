from multiprocessing import Pool
import multiprocessing
import redis
import json
from app import app, redis_connection_pool
from process_tweet import ProcessTweet
import time
from copy import copy
import pdb

def process_tweet(tweet):
    current_process = multiprocessing.current_process()
    print("Process {} is now processing tweet with id {}".format(current_process.name, tweet['id_str']))

    # Strip json
    tweet_stripped = ProcessTweet.strip(copy(tweet))

    # Todo: incorporate filter function

    # compute average location from bounding box (reducing storage on ES)

    if tweet['place'] is not None and tweet['place']['bounding_box'] is not None:
        tweet_stripped = ProcessTweet.compute_average_location(tweet, tweet_stripped)

    # If vaccine sentiment put on vaccine sentiment queue

    # else submit to ES index

    return

def queue_name(name):
    return "{}:{}".format(app.config['REDIS_NAMESPACE'], name)

def main(parallel=True, with_sleep=False):

    # instantiate Redis
    redis_conn = redis.Redis(connection_pool=redis_connection_pool)

    logstash_queue = app.config['REDIS_LOGSTASH_QUEUE_KEY']

    if parallel:
        print("Starting worker pool...")
        preprocess_pool = Pool(processes=app.config['NUM_PROCESSES_PREPROCESSING'])

        while True:
            print('Fetching new work...')

            _, _tweet = redis_conn.blpop(queue_name(logstash_queue))
            tweet = json.loads(_tweet)
            res = preprocess_pool.apply_async(process_tweet, args=(tweet,))
    else:
        # for testing purposes...
        while True:
            print('Fetching new work...')
            if with_sleep:
                print('...and sleeping for a bit')
                time.sleep(0.2)
            _, _tweet = redis_conn.blpop(queue_name(logstash_queue))
            tweet = json.loads(_tweet)
            res = process_tweet(tweet)


if __name__ == '__main__':
    main(parallel=False, with_sleep=True)
