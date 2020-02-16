from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.settings import Config
from app.stream.stream_config_reader import StreamConfigReader
from app.stream.s3_handler import S3Handler
from app.stream.redis_s3_queue import RedisS3Queue
from app.stream.es_queue import ESQueue
from app.utils.mailer import StreamStatusMailer
from app.extensions import es
from app.stream.trending_tweets import TrendingTweets
from helpers import report_error
import logging
import os
import json
import datetime
import uuid


@celery.task(name='s3-upload-task', ignore_result=True)
def send_to_s3(debug=False):
    logger = get_logger(debug)
    s3_handler = S3Handler()
    redis_queue = RedisS3Queue()
    logger.info('Pushing tweets to S3')
    project_keys = redis_queue.find_projects_in_queue()
    stream_config_reader = StreamConfigReader()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    for key in project_keys:
        project = key.decode().split(':')[-1]
        logger.info('Found {} new tweet(s) in project {}'.format(redis_queue.num_elements_in_queue(key), project))
        stream_config = stream_config_reader.get_config_by_project(project)
        tweets = b'\n'.join(redis_queue.pop_all(key))  # create json lines byte string
        now = datetime.datetime.now()
        s3_key = 'tweets/{}/{}/tweets-{}-{}.jsonl'.format(stream_config['es_index_name'], now.strftime("%Y-%m-%d"), now.strftime("%Y%m%d%H%M%S"), str(uuid.uuid4()))
        if s3_handler.upload_to_s3(tweets, s3_key):
            logging.info('Successfully uploaded file {} to S3'.format(s3_key))
        else:
            logging.error('ERROR: Upload of file {} to S3 not successful'.format(s3_key))


@celery.task(name='es-bulk-index-task', ignore_result=True)
def es_bulk_index(debug=False):
    logger = get_logger(debug)
    es_queue = ESQueue()
    stream_config_reader = StreamConfigReader()
    project_keys = es_queue.find_projects_in_queue()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    data = []
    for key in project_keys:
        tweets = es_queue.pop_all(key)
        if len(tweets) == 0:
            continue
        project = key.decode().split(':')[-1]
        stream_config = stream_config_reader.get_config_by_project(project)
        logger.info(f'Found {len(tweets):,} tweets in queue for project {project}.')
        # decode tweets
        tweets = [json.loads(t.decode()) for t in tweets]
        actions = [
            {'_id': t['id'],
            '_type': 'tweet',
            '_source': t,
            '_index': stream_config['es_index_name']
            } for t in tweets]
        data.extend(actions)
    # bulk index
    num_docs = len(data)
    if num_docs > 0:
        batch_size = 1000
        logger.info(f'Bulk-indexing {num_docs:,} documents to Elasticsearch...')
        for i in range(0, num_docs, batch_size):
            try:
                es.bulk_index(data[i:(i+batch_size)])
            except:
                report_error(logger, exception=True)
                es_queue.dump_to_disk(data[i:(i+batch_size)])
    else:
        logger.info(f'No documents to index for Elasticsearch')

@celery.task(name='trending-tweets-cleanup', ignore_result=True)
def trending_tweets_cleanup_job(debug=False):
    logger = get_logger(debug)
    # Cleanup (remove old trending tweets from redis)
    stream_config_reader = StreamConfigReader()
    for project_config in stream_config_reader.read():
        if project_config['compile_trending_tweets']:
            tt = TrendingTweets(project_config['slug'])
            tt.cleanup()

# ------------------------------------------
# EMAIL TASKS
@celery.task(name='stream-status-daily', ignore_result=True)
def stream_status_daily(debug=False):
    config = Config()
    logger = get_logger(debug)
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'test-email':
        mailer = StreamStatusMailer(status_type='daily')
        body = mailer.get_full_html()
        mailer.send_status(body)
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_counts(older_than=90)

@celery.task(name='stream-status-weekly', ignore_result=True)
def stream_status_weekly(debug=False):
    config = Config()
    logger = get_logger(debug)
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'test-email':
        mailer = StreamStatusMailer(status_type='weekly')
        body = mailer.get_body()
        mailer.send_status(body)
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_counts(older_than=90)

# ------------------------------------------
# Helper functions
def get_logger(debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    return logger
