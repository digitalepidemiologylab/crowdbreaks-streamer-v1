from flask import Flask, got_request_exception
from app import settings, main
from app.pipeline import pipeline
from app.es_interface import es_interface
import app.errors as errors
from app.ml import ml
from app.extensions import es, redis
import os
import warnings
import logging.config
import rollbar
import rollbar.contrib.flask


def create_app(config=settings.ProdConfig):
    app = Flask(__name__)

    # Configs
    app.config.from_object(config)
    validate_configs()
    logging.config.fileConfig('logging.conf')

    # Initialize extensions
    redis.init_app(app)
    es.init_app(app)

    app.register_blueprint(errors.blueprint)
    app.register_blueprint(main.blueprint, url_prefix = '/')
    app.register_blueprint(pipeline.blueprint, url_prefix = '/pipeline')
    app.register_blueprint(es_interface.blueprint, url_prefix = '/elasticsearch')
    app.register_blueprint(ml.blueprint, url_prefix = '/ml')

    # Pause stream container if running
    if app.config['PAUSE_STREAM_ON_STARTUP'] == '1':
        stop_stream(app)

    # Rollbar
    if app.config['ENV'] == 'prd':
        init_rollbar(app)
    return app


def validate_configs():
    required_envs = ['BASIC_AUTH_USERNAME', 'BASIC_AUTH_PASSWORD', 'ELASTICSEARCH_HOST', 'ELASTICSEARCH_PORT',
            'CONSUMER_KEY', 'CONSUMER_SECRET', 'OAUTH_TOKEN', 'OAUTH_TOKEN_SECRET', 'ENV']
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


def init_rollbar(app):
    app.logger.info('Initializing Rollbar')

    @app.before_first_request
    def init_rollbar():
        """init rollbar module"""
        print('setting up rollbar')
        rollbar.init(
                app.config['ROLLBAR_ACCESS_TOKEN'], # access token
                'production', # Environment name
                root=os.path.dirname(os.path.realpath(__file__)), # server root directory, makes tracebacks prettier
                allow_logging_basic_config=False
            )
        # send exceptions from `app` to rollbar, using flask's signal system.
        got_request_exception.connect(rollbar.contrib.flask.report_exception, app)

