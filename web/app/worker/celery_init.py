import os
from celery import Celery
import redis
from celery.schedules import crontab
from celery.signals import task_failure
import rollbar
from app.settings import Config

def create_celery():
    CELERY_BROKER_URL=os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
    CELERY_RESULT_BACKEND=os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

    return Celery('tasks',
            include=['app.stream.tasks', 'app.stream.beat_tasks'],
            broker=CELERY_BROKER_URL,
            backend=CELERY_RESULT_BACKEND)

celery = create_celery()

# Rollbar init
if os.environ.get('ENV') == 'prd':
    rollbar.init(os.environ.get('ROLLBAR_ACCESS_TOKEN', ''), 'production')

@task_failure.connect
def handle_task_failure(**kw):
    rollbar.report_exc_info(extra_data=kw)

# beat schedule
celery.conf.beat_schedule = {
        's3-uploads': {
            'task': 's3-upload-task',
            'schedule': 10*60.0  # runs every 10min
            },
        'email-daily': {
            'task': 'stream-status-daily',
            'schedule': crontab(hour=9, minute=0) # runs every day at 9am
            },
        'email-weekly': {
            'task': 'stream-status-weekly',
            'schedule': crontab(day_of_week=1, hour=9, minute=0) # runs at 9am on Mondays
            }
        }

config = Config()
celery.conf.timezone = config.TIMEZONE

if __name__ == "__main__":
    celery.start()
