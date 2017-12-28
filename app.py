from flask import Flask
import worker, os
import main
from pipeline import pipeline
from es_interface import es_interface
from extensions import es, redis


def create_app():
    app = Flask(__name__, instance_relative_config=True)

    # Configs
    app.config.from_object('config')

    # Secret configs
    app.config.from_pyfile('config.py')

    # Initialize extensions
    redis.init()
    es.init_app(app)

    # Blueprints
    app.register_blueprint(main.blueprint, url_prefix = '/')
    app.register_blueprint(pipeline.blueprint, url_prefix = '/pipeline')
    app.register_blueprint(es_interface.blueprint, url_prefix = '/elasticsearch')

    return app

if __name__ == '__main__':
    app = create_app()
    # os.system('python worker.py')
    app.run(use_reloader=True)
