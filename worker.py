from multiprocessing import Pool
import multiprocessing
import redis
import json
from app import app, redis_connection_pool
from process_tweet import ProcessTweet

def process_tweet(tweet):
    current_process = multiprocessing.current_process()
    print("Process {} is now processing tweet with id {}".format(current_process.name, tweet['id_str']))

    # Strip json
    tweet_stripped = ProcessTweet.strip(tweet)
    print("\n\nStripped tweet:")
    print(tweet_stripped)

    # Filter
    # Add additional fields (such as average location)
    # If vaccine sentiment put on vaccine sentiment queue
    # else submit to ES index

    return

def queue_name(name):
    return "{}:{}".format(app.config['REDIS_NAMESPACE'], name)

def main(parallel=True):

    # instantiate Redis
    redis_conn = redis.Redis(connection_pool=redis_connection_pool)

    logstash_queue = app.config['REDIS_LOGSTASH_QUEUE_KEY']

    if parallel:
        print("Starting worker pool...")
        preprocess_pool = Pool(processes=app.config['NUM_CPU_PREPROCESSING'])

        while True:
            print('Fetching new work...')
            _, _tweet = redis_conn.blpop(queue_name(logstash_queue))
            tweet = json.loads(_tweet)
            res = preprocess_pool.apply_async(process_tweet, args=(tweet,))
    else:
        while True:
            print('Fetching new work...')
            _, _tweet = redis_conn.blpop(queue_name(logstash_queue))
            tweet = json.loads(_tweet)
            res = process_tweet(tweet)


if __name__ == '__main__':
    main(parallel=True)
