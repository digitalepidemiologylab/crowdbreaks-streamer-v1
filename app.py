from flask import Flask
import redis
import elasticsearch

app = Flask(__name__, instance_relative_config=True)

# configs
app.config.from_object('config')
# secret configs
app.config.from_pyfile('config.py')

# Build connection pool to Redis
POOL = redis.ConnectionPool(host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'])

# Connect to elasticsearch
es = elasticsearch.Elasticsearch(["{}:{}".format(app.config['ELASTICSEARCH_HOST'], app.config['ELASTICSEARCH_PORT'])],
        http_auth=(app.config['ELASTICSEARCH_USERNAME'], app.config['ELASTICSEARCH_PASSWORD']))

@app.route('/', methods=['GET'])
def index():
    return "hello world!!!"


if __name__ == '__main__':
    app.run()
