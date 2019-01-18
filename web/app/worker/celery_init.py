import os
from celery import Celery
import redis
from celery.schedules import crontab
from celery.signals import task_failure
import rollbar

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
    rollbar.init(os.environ.get('ROLLBAR_ACCESS_TOKEN', '', 'production'))

@task_failure.connect
def handle_task_failure(**kw):
    rollbar.report_exc_info(extra_data=kw)

# beat schedule
celery.conf.beat_schedule = {
        's3-uploads': {
            'task': 's3-upload-task',
            'schedule': 10*60.0  # run every 10min
            },
        'email-daily': {
            'task': 'stream-status-daily',
            'schedule': crontab(hour=8, minute=0) # runs every day at 10:00 CEST (9:00 CET)
            },
        'email-weekly': {
            'task': 'stream-status-weekly',
            'schedule': crontab(day_of_week=1, hour=8, minute=0) # runs at 10:00 CEST (9:00 CET) on Mondays
            }
        }
celery.conf.timezone = 'UTC'

if __name__ == "__main__":
    celery.start()
