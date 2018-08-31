from app.settings import Config
from app.worker.celery_init import celery
from app.worker.celery_helpers import only_one
from celery.utils.log import get_task_logger
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher
import logging
import os
import json
from app.stream.s3_handler import S3Handler
import datetime
import uuid

@celery.task(ignore_result=True)
def handle_tweet(tweet, send_to_es=True, use_pq=True, debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)

    rtm = ReverseTweetMatcher(tweet=tweet)
    candidates = rtm.get_candidates()
    if len(candidates) == 0:
        logger.error('ERROR: No matching projects in tweet')
        # store to separate file for later analysis
        config = Config()
        with open(os.path.join(config.PROJECT_ROOT, 'logs', 'reverse_match_errors', tweet['id_str'] + '.json'), 'w') as f:
            json.dump(tweet, f)
        return
    else:
        logger.info("SUCCESS: Found {} project(s) ({}) as a matching project for tweet".format(len(candidates), candidates))
        s3_handler = S3Handler()
        for c in candidates:
            s3_handler.push_to_queue(tweet, c)


@celery.task(name='s3-upload-task', ignore_result=True)
def send_to_s3(debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    s3_handler = S3Handler()
    logger.info('Pushing tweets to S3')
    project_keys = s3_handler.find_projects_in_queue()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    for key in project_keys:
        project = key.decode().split(':')[-1]
        logger.info('Found {} new tweet(s) in project {}'.format(s3_handler.num_elements_in_queue(key), project))
        tweets = b'\n'.join(s3_handler.pop_all(key))  # create json lines byte string
        now = datetime.datetime.now()
        s3_key = 'tweets/{}/{}/tweets-{}-{}.jsonl'.format(project, now.strftime("%Y-%m-%d"), str(uuid.uuid4()), now.strftime("%Y%m%d%H%M%S"))
        # @future me: It would probably be a good idea to compress data before upload...
        if s3_handler.upload_to_s3(tweets, s3_key):
            logging.info('Successfully uploaded file {} to S3'.format(s3_key))
        else:
            logging.error('ERROR: Upload of file {} to S3 not successful'.format(s3_key))


@celery.task
def predict(text, model='fasttext_v1.ftz', num_classes=3, path_to_model='.'):
    logger = get_task_logger(__name__)
    model_path = os.path.join(os.path.abspath(path_to_model), 'bin', 'vaccine_sentiment', model)
    m = fastText.load_model(model_path)
    pred = m.predict(text, k=num_classes)
    label_dict = {'__label__-1': ['anti-vaccine', -1], '__label__0': ['neutral', 0], '__label__1': ['pro-vaccine', 1]}
    return { 'labels': [label_dict[l][0] for l in pred[0]], 'label_vals': [label_dict[l][1] for l in pred[0]],
            'probabilities': list(pred[1]), 'model': model.split('.')[0]}
