from app.settings import Config
from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher
from app.utils.process_tweet import ProcessTweet
from app.utils.process_media import ProcessMedia
from app.utils.priority_queue import TweetIdQueue
from app.utils.project_config import ProjectConfig
from app.utils.redis import Redis
from app.utils.data_dump_ids import DataDumpIds
from app.stream.redis_s3_queue import RedisS3Queue
from app.stream.trending_tweets import TrendingTweets
from app.stream.trending_topics import TrendingTopics
from app.stream.es_queue import ESQueue
from app.extensions import es
import logging
import os
import json

config = Config()

@celery.task(ignore_result=True)
def handle_tweet(tweet, send_to_es=True, use_pq=True, debug=False, store_unmatched_tweets=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    # reverse match to find project
    rtm = ReverseTweetMatcher(tweet=tweet)
    candidates = rtm.get_candidates()
    tweet_id = tweet['id_str']
    # open Redis connection only once
    # redis = Redis()
    # connection = redis.get_connection()
    connection = None
    if len(candidates) == 0:
        # Could not match keywords. This might occur quite frequently e.g. when tweets are collected accross different languages/keywords
        logger.info(f'Tweet {tweet_id} could not be matched against any existing projects.')
        if store_unmatched_tweets:
            # store to separate file for later analysis
            with open(os.path.join(config.PROJECT_ROOT, 'logs', 'reverse_match_errors', f'{tweet_id}.json'), 'w') as f:
                json.dump(tweet, f)
        return
    # queue up for s3 upload and add to priority queue
    logger.info("SUCCESS: Found {} project(s) ({}) as a matching project for tweet".format(len(candidates), ', '.join(candidates)))
    redis_queue = RedisS3Queue(connection=connection)
    es_queue = ESQueue(connection=connection)
    stream_config_reader = ProjectConfig()
    for project in candidates:
        stream_config = stream_config_reader.get_config_by_slug(project)
        if stream_config['storage_mode'] == 'test_mode':
            logger.debug('Running in test mode. Not sending to S3 or ES.')
            return
        # add tracking info
        tweet['_tracking_info'] = stream_config_reader.get_tracking_info(project)
        tweet['_tracking_info']['matching_keywords'] = rtm.matching_keywords[project]
        # Queue up on Redis for subsequent upload
        redis_queue.push(json.dumps(tweet).encode(), project)
        # preprocess tweet
        pt = ProcessTweet(tweet, project_locales=stream_config['locales'])
        pt.process()
        # Possibly add tweet to trending tweets
        if stream_config['compile_trending_tweets']:
            trending_tweets = TrendingTweets(project, project_locales=stream_config['locales'], connection=connection)
            trending_tweets.process(tweet)
        # Extract trending topics
        if stream_config['compile_trending_topics']:
            trending_topics = TrendingTopics(project, project_locales=stream_config['locales'], project_keywords=stream_config['keywords'], connection=connection)
            trending_topics.process(tweet)
        if stream_config['compile_data_dump_ids'] and config.ENV == 'prd':
            data_dump_ids = DataDumpIds(project, connection=connection)
            data_dump_ids.add(tweet_id)
            if pt.has_place:
                data_dump_ids = DataDumpIds(project, mode='has_place', connection=connection)
                data_dump_ids.add(tweet_id)
            if pt.has_coordinates:
                data_dump_ids = DataDumpIds(project, mode='has_coordinates', connection=connection)
                data_dump_ids.add(tweet_id)
        if use_pq and pt.should_be_annotated():
            # add to Tweet ID queue for crowd labelling
            logger.info(f'Add tweet {tweet_id} to priority queue...')
            processed_tweet = pt.get_processed_tweet()
            tid = TweetIdQueue(stream_config['es_index_name'], priority_threshold=3, connection=connection)
            processed_tweet['text'] = pt.get_text(anonymize=True)
            tid.add_tweet(tweet_id, processed_tweet, priority=0)
        if stream_config['image_storage_mode'] != 'inactive':
            pm = ProcessMedia(tweet, project, image_storage_mode=stream_config['image_storage_mode'])
            pm.process()
        if send_to_es and stream_config['storage_mode'] in ['s3-es', 's3-es-no-retweets']:
            if rtm.is_retweet and stream_config['storage_mode'] == 's3-es-no-retweets':
                # Do not store retweets on ES
                return
            # send to ES
            processed_tweet = pt.get_processed_tweet()
            logger.debug(f'Pushing processed with id {tweet_id} to ES queue')
            es_tweet_obj = {'processed_tweet': processed_tweet, 'id': tweet_id}
            if len(stream_config['model_endpoints']) > 0:
                # prepare for prediction
                es_tweet_obj['text_for_prediction'] = {'text': pt.get_text(anonymize=True), 'id': tweet_id}
            es_queue.push(json.dumps(es_tweet_obj).encode(), project)
