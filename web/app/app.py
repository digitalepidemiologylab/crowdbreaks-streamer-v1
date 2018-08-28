from flask import Flask
from app import settings, main
from app.pipeline import pipeline
from app.es_interface import es_interface
from app.extensions import es, redis
import os
import warnings
import logging.config


def create_app(config=settings.ProdConfig):
    app = Flask(__name__)

    # Configs
    app.config.from_object(config)
    validate_configs()
    logging.config.fileConfig('logging.conf')

    # Initialize extensions
    redis.init_app(app)
    es.init_app(app)

    # Blueprints
    app.register_blueprint(main.blueprint, url_prefix = '/')
    app.register_blueprint(pipeline.blueprint, url_prefix = '/pipeline')
    app.register_blueprint(es_interface.blueprint, url_prefix = '/elasticsearch')

    # Pause stream container if running
    if app.config['PAUSE_STREAM_ON_STARTUP'] == '1':
        stop_stream(app)

    return app


def validate_configs():
    required_envs = ['BASIC_AUTH_USERNAME', 'BASIC_AUTH_PASSWORD', 'ELASTICSEARCH_HOST', 'ELASTICSEARCH_PORT',
            'CONSUMER_KEY', 'CONSUMER_SECRET', 'OAUTH_TOKEN', 'OAUTH_TOKEN_SECRET']
    for r in required_envs:
        if r not in os.environ:
            warnings.warn('Environment variable "{}" needs to be set.'.format(r), RuntimeWarning)

def stop_stream(app):
    d = pipeline.DockerWrapper()
    if d.container_status(app.config['STREAM_DOCKER_CONTAINER_NAME']) == 'running':
        try:
            d.pause_container(app.config['STREAM_DOCKER_CONTAINER_NAME'])
        except Exception as e:
            app.logger.warning('Something went wrong when trying to pause the stream container.')
            app.logger.warning(e)
        else:
            app.logger.info('Successfully paused stream container')
