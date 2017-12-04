from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from basic_auth import requires_auth
import json
import redis
import elasticsearch
import sys
from logger import Logger
from elastic_search.elastic import Elastic
import pdb
import worker
import os

app = Flask(__name__, instance_relative_config=True)

# Allow cross origin requests (development)
CORS(app)

# Configs
app.config.from_object('config')

# Secret configs
app.config.from_pyfile('config.py')

# Create logger instance
logger = Logger.setup('app')
 
# Build connection pool to Redis
if 'REDIS_PW' in app.config:
    POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
            port=app.config['REDIS_PORT'],
            db=app.config['REDIS_DB'],
            password=app.config['REDIS_PW'])
else:
    POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
            port=app.config['REDIS_PORT'],
            db=app.config['REDIS_DB'])

# Test Redis
redis_conn = redis.Redis(connection_pool=POOL)
if redis_conn.ping():
    logger.info('Successfully connected to Redis')
else:
    logger.error('Connection to Redis not successful')

# Connect to elasticsearch
es = Elastic()

@app.route('/', methods=['GET'])
def index():
    return "hello world!!!"

@app.route('/test/redis', methods=['GET'])
@requires_auth
def test_redis():
    return json.dumps(redis_conn.ping())

@app.route('/test/es', methods=['GET'])
@requires_auth
def test_es():
    return json.dumps(es.test_connection())

@app.route('/sentiment/vaccine', methods=['POST'])
@requires_auth
def get_vaccine_sentiment():
    data = request.get_json()
    logger.debug('Incoing request with data {}'.format(data))
    label, distances = worker.vaccine_sentiment_single_request(data, logger)
    res = {'label': label, 'distances': distances}
    logger.debug('Result: {}'.format(label))
    return json.dumps(res)


@app.route('/sentiment/data/<value>', methods=['GET'])
@requires_auth
def get_vaccine_data(value):
    options = {}
    options['interval'] = request.args.get('interval', 'month')
    options['start_date'] = request.args.get('start_date', 'now-20y')
    options['end_date'] = request.args.get('end_date', 'now')
    res = es.get_sentiment_data('project_vaccine_sentiment', value, **options)
    return json.dumps(res)


@app.route('/sentiment/data/all', methods=['GET'])
@requires_auth
def get_all_data():
    options = {}
    options['interval'] = request.args.get('interval', 'month')
    options['start_date'] = request.args.get('start_date', 'now-20y')
    options['end_date'] = request.args.get('end_date', 'now')

    res = es.get_all_agg('project_vaccine_sentiment', **options)
    return json.dumps(res)


if __name__ == '__main__':
    os.system('python worker.py')
    app.run(host='0.0.0.0')
