from flask import Flask
import redis
import elasticsearch
import sys
from logger import Logger

app = Flask(__name__, instance_relative_config=True)

# configs
app.config.from_object('config')
# secret configs
app.config.from_pyfile('config.py')

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
es = elasticsearch.Elasticsearch(["{}:{}".format(app.config['ELASTICSEARCH_HOST'], app.config['ELASTICSEARCH_PORT'])],
        http_auth=(app.config['ELASTICSEARCH_USERNAME'], app.config['ELASTICSEARCH_PASSWORD']))
if es.ping():
    logger.info('Successfully connected to ElasticSearch')
else:
    logger.error('Connection to ElasticSearch not successful')


@app.route('/', methods=['GET'])
def index():
    return "hello world!!!"


if __name__ == '__main__':
    app.run()
