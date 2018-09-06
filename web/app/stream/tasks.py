from app.settings import Config
from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher
from app.utils.process_tweet import ProcessTweet
from app.utils.priority_queue import TweetIdQueue
from app.utils.predict_sentiment import PredictSentiment
from app.stream.stream_config_reader import StreamConfigReader
from app.stream.redis_s3_queue import RedisS3Queue
from app.extensions import es
import logging
import os
import json

@celery.task(ignore_result=True)
def handle_tweet(tweet, send_to_es=True, use_pq=True, debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    # reverse match to find project
    rtm = ReverseTweetMatcher(tweet=tweet)
    candidates = rtm.get_candidates()
    if len(candidates) == 0:
        logger.error('ERROR: No matching projects in tweet')
        # store to separate file for later analysis
        config = Config()
        with open(os.path.join(config.PROJECT_ROOT, 'logs', 'reverse_match_errors', tweet['id_str'] + '.json'), 'w') as f:
            json.dump(tweet, f)
        return
    logger.info("SUCCESS: Found {} project(s) ({}) as a matching project for tweet".format(len(candidates), ', '.join(candidates)))
    # queue up for s3 upload and add to priority queue
    redis_queue = RedisS3Queue()
    stream_config_reader = StreamConfigReader()
    for project in candidates:
        stream_config = stream_config_reader.get_config_by_project(project)
        if stream_config['storage_mode'] == 'test':
            logger.debug('Running in test mode. Not sending to S3 or ES.')
            return
        # add tracking info
        tweet['_tracking_info'] = stream_config_reader.get_tracking_info(project)
        # queue up on Redis for subsequent upload
        redis_queue.push(tweet, project)
        if use_pq and not rtm.is_retweet:
            # add to Tweet ID queue for crowd labelling
            logger.debug('Add tweet to priority queue...')
            tid = TweetIdQueue(project, priority_threshold=3)
            tid.add(tweet['id'], priority=0)
        if send_to_es and stream_config['storage_mode'] in ['s3-es', 's3-es-no-retweets']:
            if rtm.is_retweet and stream_config['storage_mode'] == 's3-es-no-retweets':
                # Do not store retweets on ES
                return
            # process tweet
            pt = ProcessTweet(tweet=tweet, project=project)
            processed_tweet = pt.process()
            # send to ES
            logger.debug('Sending tweet with id {} to ES'.format(processed_tweet['id']))
            es.index_tweet(processed_tweet, stream_config['es_index_name'])

@celery.task
def predict(text, model='fasttext_v1.ftz', num_classes=3, path_to_model='.'):
    ps = PredictSentiment()
    prediction = ps.predict(text, model=model)
    return prediction
