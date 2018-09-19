import os
from celery import Celery
import redis
from celery.schedules import crontab

def create_celery():
    CELERY_BROKER_URL=os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
    CELERY_RESULT_BACKEND=os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

    return Celery('tasks',
            include=['app.stream.tasks', 'app.stream.beat_tasks'],
            broker=CELERY_BROKER_URL,
            backend=CELERY_RESULT_BACKEND)

celery = create_celery()

# beat schedule
celery.conf.beat_schedule = {
        's3-uploads': {
            'task': 's3-upload-task',
            'schedule': 10*60.0  # run every 10min
            },
        'email-daily': {
            'task': 'stream-status-daily',
            'schedule': crontab(hour=10, minute=0) # run every day at 10:00
            },
        'email-weekly': {
            'task': 'stream-status-weekly',
            'schedule': crontab(day_of_week=1, hour=10, minute=0) # on mondays 10:00
            }
        }
celery.conf.timezone = 'CET'


if __name__ == "__main__":
    celery.start()
