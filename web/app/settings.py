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
    REDIS_HOST='localhost'
    REDIS_HOST=os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT=os.environ.get('REDIS_PORT', 6379)
    REDIS_DB=os.environ.get('REDIS_DB', 0)


    # logstash
    LOGSTASH_CONFIG_FILE='pipeline.conf'


class ProdConfig(Config):
    """Production configuration."""
    ENV = 'prod'
    DEBUG = False
    LOGSTASH_CONFIG_PATH='/etc/logstash/conf.d'


class StgConfig(Config):
    """Production configuration."""
    ENV = 'stg'
    DEBUG = False
    LOGSTASH_CONFIG_PATH='/etc/logstash/conf.d'


class DevConfig(Config):
    """Development configuration."""
    ENV = 'dev'
    DEBUG = True
    LOGSTASH_CONFIG_PATH='/Usersc/martin/projects/crowdbreaks-flask-api/pipeline/config'


class TestConfig(Config):
    """Test configuration."""
    TESTING = True
    DEBUG = True
