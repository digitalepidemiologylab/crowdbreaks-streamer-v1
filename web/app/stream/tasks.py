from app.settings import Config
from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher
import logging
import os
import json

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
        for c in candidates:
            send_to_s3.delay(tweet, c, debug=debug)


@celery.task(ignore_result=True)
def send_to_s3(tweet, project, debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)

    logger.debug('Sending tweet {} by project {} to S3...'.format(tweet['id_str'], project))
    return



@celery.task
def predict(text, model='fasttext_v1.ftz', num_classes=3, path_to_model='.'):
    logger = get_task_logger(__name__)
    model_path = os.path.join(os.path.abspath(path_to_model), 'bin', 'vaccine_sentiment', model)
    m = fastText.load_model(model_path)
    pred = m.predict(text, k=num_classes)
    label_dict = {'__label__-1': ['anti-vaccine', -1], '__label__0': ['neutral', 0], '__label__1': ['pro-vaccine', 1]}
    return { 'labels': [label_dict[l][0] for l in pred[0]], 'label_vals': [label_dict[l][1] for l in pred[0]],
            'probabilities': list(pred[1]), 'model': model.split('.')[0]}
