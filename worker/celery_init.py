import os
from celery import Celery

def create_celery():
    env=os.environ
    CELERY_BROKER_URL=env.get('CELERY_BROKER_URL', 'redis://localhost:6379'),
    CELERY_RESULT_BACKEND=env.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379')

    return Celery('tasks', include=['worker.tasks'], broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)


celery = create_celery()


if __name__ == "__main__":
    celery.start()
