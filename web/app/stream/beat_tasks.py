from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.settings import Config
from app.stream.stream_config_reader import StreamConfigReader
from app.stream.s3_handler import S3Handler
from app.stream.redis_s3_queue import RedisS3Queue
from app.utils.mailer import StreamStatusMailer
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

# ------------------------------------------
# EMAIL TASKS
@celery.task(name='stream-status-daily', ignore_result=True)
def stream_status_daily(debug=False):
    config = Config()
    logger = get_logger(debug)
    # if config.SEND_EMAILS == '1' and config.ENV == 'prd':
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'dev':
        mailer = StreamStatusMailer(status_type='daily')
        body = mailer.get_body_daily()
        mailer.compose_message(body)
        mailer.send_status()
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_daily_counts(older_than=90)

# EMAIL TASKS
@celery.task(name='stream-status-weekly', ignore_result=True)
def stream_status_weekly(debug=False):
    config = Config()
    logger = get_logger(debug)
    # if config.SEND_EMAILS == '1' and config.ENV == 'prd':
    print(config.ENV)
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'dev':
        mailer = StreamStatusMailer(status_type='weekly')
        body = mailer.get_body_daily()
        mailer.compose_message(body)
        mailer.send_status()
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_daily_counts(older_than=90)

# ------------------------------------------
# Helper functions
def get_logger(debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    return logger
