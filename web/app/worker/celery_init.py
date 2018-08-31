import os
from celery import Celery
import redis

def create_celery():
    CELERY_BROKER_URL=os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
    CELERY_RESULT_BACKEND=os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

    return Celery('tasks',
            include=['app.worker.tasks', 'app.stream.tasks'],
            broker=CELERY_BROKER_URL,
            backend=CELERY_RESULT_BACKEND)

celery = create_celery()

# beat schedule
celery.conf.beat_schedule = {
        's3-uploads': {
            'task': 's3-upload-task',
            'schedule': 5*60.0  # run every 5min
            }
        }
celery.conf.timezone = 'UTC'


if __name__ == "__main__":
    celery.start()
