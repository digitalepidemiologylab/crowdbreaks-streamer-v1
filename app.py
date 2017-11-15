from flask import Flask, jsonify
import redis
import elasticsearch
import sys
from logger import Logger
from elastic_search.elastic import Elastic

app = Flask(__name__, instance_relative_config=True)

# configs
app.config.from_object('config')
# secret configs
app.config.from_pyfile('config.py')

# create logger instance
logger = Logger.setup('app')
 
# Build connection pool to Redis
POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'])
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

@app.route('/elasticsearch/indices', methods=['GET'])
def indeces():
    return jsonify(es.indices.get_alias('*'))

def index():
    return "hello world!!!"

if __name__ == '__main__':
    app.run()
