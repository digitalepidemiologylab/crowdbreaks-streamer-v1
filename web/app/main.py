from flask import Flask, request, Blueprint, Response
from app.basic_auth import requires_auth_func
import json
# from worker.worker import vaccine_sentiment_single_request
from app.extensions import es, redis
import logging
from app.worker.priority_queue import TweetIdQueue
import time


blueprint = Blueprint('main', __name__)
logger = logging.getLogger('Main')


@blueprint.before_request
def require_auth_all():
    return requires_auth_func()


@blueprint.route('/', methods=['GET'])
def index():
    return "hello world!!!"


@blueprint.route('test/redis', methods=['GET'])
def test_redis():
    return json.dumps(redis.test_connection())


@blueprint.route('test/es', methods=['GET'])
def test_es():
    return json.dumps(es.test_connection())


@blueprint.route('tweet/new/<project>', methods=['GET'])
def get_new_tweet(project):
    """"Get new tweet from priority queue"""
    user_id = request.args.get('user_id', None)
    tid = TweetIdQueue(project)
    tweet_id = tid.get(user_id=user_id)
    if tweet_id is None:
        logger.error('Could not get tweet id from priority queue. Getting random tweet from ES instead.')
        # get a random tweet instead
        tweet_id = es.get_random_document_id(project, seed=int(time.time()*1000))
        if tweet_id is None:
            logger.error('Could not get random tweet from elasticsearch.')
            return Response(None, status=400, mimetype='text/plain')
    return Response(str(tweet_id), status=200, mimetype='text/plain')


@blueprint.route('tweet/update/<project>', methods=['POST'])
def add_to_pq(project):
    """Update priority score in queue and remember that a user has already classified a tweet"""
    data = request.get_json()
    logger.debug('Incoing request with data {}'.format(data))
    if data is None or 'user_id' not in data or 'tweet_id' not in data:
        logger.error('No user_id was passed when updating ')
        return Response(None, status=400)
    tid = TweetIdQueue(project)
    tid.update(data['tweet_id'], data['user_id'])
    return Response('Update successful.', status=200, mimetype='text/plain')


# @blueprint.route('sentiment/vaccine', methods=['POST'])
# def get_vaccine_sentiment():
#     data = request.get_json()
#     logger.debug('Incoing request with data {}'.format(data))
#     label, distances = vaccine_sentiment_single_request(data, logger)
#     res = {'label': label, 'distances': distances}
#     logger.debug('Result: {}'.format(label))
#     return json.dumps(res)
#

@blueprint.route('sentiment/data/<value>', methods=['GET'])
def get_vaccine_data(value):
    options = get_params(request.args)
    res = es.get_sentiment_data('project_vaccine_sentiment', value, **options)
    return json.dumps(res)


@blueprint.route('data/all/<index_name>', methods=['GET'])
def get_all_data(index_name):
    options = get_params(request.args)
    res = es.get_all_agg(index_name, **options)
    return json.dumps(res)


def get_params(args):
    options = {}
    # dates must be of format 'yyyy-MM-dd H:m:s'
    options['interval'] = args.get('interval', 'month')
    options['start_date'] = args.get('start_date', 'now-20y')
    options['end_date'] = args.get('end_date', 'now')
    return options
