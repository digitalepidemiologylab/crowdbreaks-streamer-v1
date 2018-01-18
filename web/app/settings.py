# -*- coding: utf-8 -*-
"""Application configuration."""
import os

class Config(object):
    """Base configuration."""

    APP_DIR = os.path.abspath(os.path.dirname(__file__))  # This directory
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_DIR, os.pardir))
    BASIC_AUTH_USERNAME = os.environ.get('BASIC_AUTH_USERNAME')
    BASIC_AUTH_PASSWORD = os.environ.get('BASIC_AUTH_PASSWORD')

    # Elasticsearch
    ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST')
    ELASTICSEARCH_PORT = os.environ.get('ELASTICSEARCH_PORT')
    ELASTICSEARCH_USERNAME= os.environ.get('ELASTICSEARCH_USERNAME', None)
    ELASTICSEARCH_PASSWORD = os.environ.get('ELASTICSEARCH_PASSWORD', None)

    # Redis
    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
    REDIS_DB = os.environ.get('REDIS_DB', 0)

    # Redis queue keys/namespaces
    REDIS_NAMESPACE = os.environ.get('REDIS_NAMESPACE', 'cb')
    REDIS_LOGSTASH_QUEUE_KEY = os.environ.get('REDIS_LOGSTASH_QUEUE_KEY', 'logstash')


    # logstash
    LOGSTASH_OUTPUT_FILE='output.conf'
    LOGSTASH_FILTER_FILE='filter.conf'
    LOGSTASH_DOCKER_CONTAINER_NAME='logstash'
    LOGSTASH_CONFIG_PATH='/logstash/config'

    # Twitter API
    CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
    CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')
    OAUTH_TOKEN = os.environ.get('OAUTH_TOKEN')
    OAUTH_TOKEN_SECRET = os.environ.get('OAUTH_TOKEN_SECRET')


class ProdConfig(Config):
    """Production configuration."""
    ENV = 'prod'
    DEBUG = False


class DevConfig(Config):
    """Development configuration."""
    ENV = 'dev'
    DEBUG = True


class TestConfig(Config):
    """Test configuration."""
    TESTING = True
    DEBUG = True
