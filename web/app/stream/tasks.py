from app.settings import Config
from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher
from app.utils.process_tweet import ProcessTweet
from app.utils.priority_queue import TweetIdQueue
from app.utils.predict_sentiment import PredictSentiment
from app.stream.stream_config_reader import StreamConfigReader
from app.stream.s3_handler import S3Handler
from app.extensions import es
import logging
import os
import json
import datetime
import uuid

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
    s3_handler = S3Handler()
    stream_config_reader = StreamConfigReader()
    for project in candidates:
        stream_config = stream_config_reader.get_config_by_project(project)
        if stream_config['storage_mode'] == 'test':
            logger.debug('Running in test mode. Not sending to S3 or ES.')
            return
        # add tracking info
        tweet['_tracking_info'] = stream_config_reader.get_tracking_info(project)
        s3_handler.push_to_queue(tweet, project)
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

@celery.task(name='s3-upload-task', ignore_result=True)
def send_to_s3(debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    s3_handler = S3Handler()
    logger.info('Pushing tweets to S3')
    project_keys = s3_handler.find_projects_in_queue()
    stream_config_reader = StreamConfigReader()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    for key in project_keys:
        project = key.decode().split(':')[-1]
        logger.info('Found {} new tweet(s) in project {}'.format(s3_handler.num_elements_in_queue(key), project))
        stream_config = stream_config_reader.get_config_by_project(project)
        tweets = b'\n'.join(s3_handler.pop_all(key))  # create json lines byte string
        now = datetime.datetime.now()
        s3_key = 'tweets/{}/{}/tweets-{}-{}.jsonl'.format(stream_config['es_index_name'], now.strftime("%Y-%m-%d"), now.strftime("%Y%m%d%H%M%S"), str(uuid.uuid4()))
        # @future me: It would probably be a good idea to compress data before upload...
        if s3_handler.upload_to_s3(tweets, s3_key):
            logging.info('Successfully uploaded file {} to S3'.format(s3_key))
        else:
            logging.error('ERROR: Upload of file {} to S3 not successful'.format(s3_key))

@celery.task
def predict(text, model='fasttext_v1.ftz', num_classes=3, path_to_model='.'):
    ps = PredictSentiment()
    prediction = ps.predict(text, model=model)
    return prediction
