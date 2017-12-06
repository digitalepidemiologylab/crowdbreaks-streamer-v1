from flask import Flask
import worker, os
import main
from connections import elastic, redis


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    # Configs
    app.config.from_object('config')

    # Secret configs
    app.config.from_pyfile('config.py')

    # Connect to Redis (builds connection pool)
    r = redis.Redis()
    r.init()

    # Connect to elasticsearch
    es = elastic.Elastic()
    es.init()

    # Blueprints
    app.register_blueprint(main.blueprint, url_prefix = '/')

    return app

if __name__ == '__main__':
    app = create_app()
    os.system('python worker.py')
    app.run(use_reloader=False)
