import logging
import os

# slightly hackish but should work
if not os.path.exists('logs'):
    os.makedirs('logs')

LOGGING_CONFIG = {
        'version': 1,
        'formatters': {
            'verbose': {
                'format': "%(asctime)s [%(levelname)-5.5s] [%(name)-9.9s]: %(message)s"
                }
            },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
                'level': logging.DEBUG
                },
            "error_file_handler": {
                'class': "logging.handlers.RotatingFileHandler",
                'level': logging.ERROR,
                'formatter': 'verbose',
                'filename': './logs/errors.log',
                'maxBytes': 10485760,
                'encoding': 'utf-8',
                'backupCount': 3
                },
            "all_logs_file_handler": {
                'class': "logging.handlers.RotatingFileHandler",
                'level': logging.DEBUG,
                'formatter': 'verbose',
                'filename': './logs/all.log',
                'maxBytes': 10485760,
                'encoding': 'utf-8',
                'backupCount': 3
                }
            },
        'loggers': {
            'ES': {
                'level': logging.DEBUG
                },
            'Redis': {
                'level': logging.DEBUG
                },
            'Main': {
                'level': logging.DEBUG
                },
            'Pipeline': {
                'level': logging.DEBUG
                },
            'ES interface': {
                'level': logging.DEBUG
                },
            'worker': {
                'level': logging.DEBUG
                },
            'PriorityQueue': {
                'level': logging.DEBUG
                },
            'PrioritySet': {
                'level': logging.DEBUG
                },
            'gunicorn.access': {
                'level': logging.DEBUG
                },
            'gunicorn.error': {
                'level': logging.DEBUG
                },
            },
        'root': {
            'handlers': ['console', 'error_file_handler', 'all_logs_file_handler'],
            'level': logging.DEBUG,
            }
        }
