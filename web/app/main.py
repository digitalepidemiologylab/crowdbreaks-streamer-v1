from flask import Flask, request, Blueprint, Response, jsonify
from flask import current_app as app
from app.basic_auth import requires_auth_func
import json
from app.extensions import es, redis
import logging
from app.utils.priority_queue import TweetIdQueue
from app.utils.predict_sentiment import PredictSentiment
from app.stream.tasks import predict, handle_tweet
import time
from statsmodels.nonparametric.smoothers_lowess import lowess
import numpy as np
import os


blueprint = Blueprint('main', __name__)
logger = logging.getLogger('Main')


@blueprint.before_request
def require_auth_all():
    return requires_auth_func()


@blueprint.route('/', methods=['GET'])
def index():
    return "hello world!!!"


#################################################################
# TESTS
@blueprint.route('test/redis', methods=['GET'])
def test_redis():
    return json.dumps(redis.test_connection())


@blueprint.route('test/celery', methods=['GET'])
def test_celery():
    with open(os.path.join(app.config['CONFIG_PATH'], 'example_data', 'tweet.json'), 'r') as f:
        tweet = json.load(f)
    handle_tweet(tweet, send_to_es=False, use_pq=False, debug=True)
    return 'testing celery'

#################################################################
# TWEET ID HANDLING
@blueprint.route('tweet/new/<project>', methods=['GET'])
def get_new_tweet(project):
    """"Get new tweet from priority queue"""
    user_id = request.args.get('user_id', None)
    tid = TweetIdQueue(project)
    tweet_id = tid.get(user_id=user_id)
    if tweet_id is None:
        logger.error('Could not get tweet id from priority queue. Getting random tweet from ES instead.')
        # get a random tweet instead
        tweet_id = es.get_random_document_id(project)
        if tweet_id is None:
            logger.error('Could not get random tweet from elasticsearch.')
            return Response(None, status=400, mimetype='text/plain')
    return Response(str(tweet_id), status=200, mimetype='text/plain')


@blueprint.route('tweet/update/<project>', methods=['POST'])
def add_to_pq(project):
    """Update priority score in queue and remember that a user has already classified a tweet"""
    data = request.get_json()
    logger.debug('Incoming request with data {}'.format(data))
    if data is None or 'user_id' not in data or 'tweet_id' not in data:
        logger.error('No user_id was passed when updating ')
        return Response(None, status=400, mimetype='text/plain')
    tid = TweetIdQueue(project)
    tid.update(data['tweet_id'], data['user_id'])
    return Response('Update successful.', status=200, mimetype='text/plain')


@blueprint.route('tweet/remove/<project>', methods=['POST'])
def remove_from_pq(project):
    """Remove a tweet which is now private"""
    data = request.get_json()
    logger.debug('Incoming request with data {}'.format(data))
    if data is None or 'tweet_id' not in data:
        logger.error('No tweet_id was passed when updating')
        return Response(None, status=400, mimetype='text/plain')
    tid = TweetIdQueue(project)
    tid.remove(data['tweet_id'])
    return Response('Successfully removed.', status=200, mimetype='text/plain')

#################################################################
# All data
@blueprint.route('data/all/<index_name>', methods=['GET'])
def get_all_data(index_name):
    options = get_params(request.args)
    res = es.get_all_agg(index_name, **options)
    return json.dumps(res)

#################################################################
# Sentiment data
@blueprint.route('sentiment/vaccine/', methods=['POST', 'GET'])
def get_vaccine_sentiment(model='fasttext_v1.ftz'):
    text = None
    if request.method == 'POST':
        data = request.get_json()
        logger.debug('Incoming request with data {}'.format(data))
        text = data.get('text', None)
        if text is None:
            return Response(None, status=400, mimetype='text/plain')
    else:
        text = 'This is just a test string'

    ps = PredictSentiment()
    prediction = ps.predict(text, model=model)
    return json.dumps(prediction)


@blueprint.route('sentiment/data/<value>', methods=['GET'])
def get_vaccine_data(value):
    options = get_params(request.args)
    res = es.get_sentiment_data('project_vaccine_sentiment', value, **options)
    for d in res:
        if d['doc_count'] == 0:
            d['doc_count'] = 'null'
    return json.dumps(res)


@blueprint.route('sentiment/average', methods=['GET'])
def get_average_sentiment():
    options = get_params(request.args)
    res = es.get_av_sentiment('project_vaccine_sentiment', **options)
    res_lowess = compute_loess(res)
    return json.dumps(res_lowess)

@blueprint.route('sentiment/geo', methods=['GET'])
def get_geo_sentiment():
    options = get_params(request.args)
    res = es.get_geo_sentiment('project_vaccine_sentiment', **options)
    return json.dumps(res)


def get_params(args):
    options = {}
    # dates must be of format 'yyyy-MM-dd HH:mm:ss'
    options['interval'] = args.get('interval', 'month')
    options['start_date'] = args.get('start_date', 'now-20y')
    options['end_date'] = args.get('end_date', 'now')
    return options

def compute_loess(data):
    y = np.array([d['avg_sentiment']['value'] for d in data])
    x = np.array([d['key'] for d in data])
    lowess_fit = lowess(y, x, frac=0.1, is_sorted=True, return_sorted=False)
    data_new = []
    for i, d in enumerate(data):
        if np.isnan(lowess_fit[i]):
            # needed for Ruby to be able to parse NaN values
            d['avg_sentiment']['value_smoothed'] = 'null'
        else: 
            d['avg_sentiment']['value_smoothed'] = lowess_fit[i]
        data_new.append(d)
    return data_new
