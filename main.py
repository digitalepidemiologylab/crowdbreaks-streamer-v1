from flask import Flask, request, Blueprint
from basic_auth import requires_auth_func
import json
from logger import Logger
from connections import elastic, redis
from worker import vaccine_sentiment_single_request


blueprint = Blueprint('main', __name__)
logger = Logger.setup('app')

@blueprint.before_request
def require_auth_all():
    requires_auth_func()


@blueprint.route('/', methods=['GET'])
def index():
    return "hello world!!!"


@blueprint.route('test/redis', methods=['GET'])
def test_redis():
    r = redis.Redis()
    return json.dumps(r.test_connection())


@blueprint.route('test/es', methods=['GET'])
def test_es():
    es = elastic.Elastic()
    return json.dumps(es.test_connection())


@blueprint.route('sentiment/vaccine', methods=['POST'])
def get_vaccine_sentiment():
    data = request.get_json()
    logger.debug('Incoing request with data {}'.format(data))
    label, distances = vaccine_sentiment_single_request(data, logger)
    res = {'label': label, 'distances': distances}
    logger.debug('Result: {}'.format(label))
    return json.dumps(res)


@blueprint.route('sentiment/data/<value>', methods=['GET'])
def get_vaccine_data(value):
    options = get_params(request.args)
    es = elastic.Elastic()
    res = es.get_sentiment_data('project_vaccine_sentiment', value, **options)
    return json.dumps(res)


@blueprint.route('data/all/<index_name>', methods=['GET'])
def get_all_data(index_name):
    options = get_params(request.args)
    es = elastic.Elastic()
    res = es.get_all_agg(index_name, **options)
    return json.dumps(res)


def get_params(args):
    options = {}
    # dates must be of format 'yyyy-MM-dd H:m:s'
    options['interval'] = args.get('interval', 'month')
    options['start_date'] = args.get('start_date', 'now-20y')
    options['end_date'] = args.get('end_date', 'now')
    return options

