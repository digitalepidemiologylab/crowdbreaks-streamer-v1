from flask import Flask, jsonify, request
from flask_cors import CORS
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
POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'],
        password=app.config['REDIS_PW'])

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


@app.route('/sentiment/vaccine', methods=['POST'])
def get_vaccine_sentiment():
    data = request.get_json()
    label, distances = worker.vaccine_sentiment_single_request(data)
    res = {'label': label, 'distances': distances}
    return json.dumps(res)


if __name__ == '__main__':
    os.system('python worker.py')
    app.run(host='0.0.0.0')
