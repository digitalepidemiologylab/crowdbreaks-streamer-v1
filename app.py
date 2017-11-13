from flask import Flask
import redis

app = Flask(__name__, instance_relative_config=True)

# configs
app.config.from_object('config')
# secret configs
app.config.from_pyfile('config.py')

# Build connection pool to Redis
redis_connection_pool = redis.ConnectionPool(host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
        db=app.config['REDIS_DB'])

@app.route('/', methods=['GET'])
def index():
    return "hello world!!!"


if __name__ == '__main__':
    app.run()
