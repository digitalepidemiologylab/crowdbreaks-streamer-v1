from app.worker.celery_init import celery
from celery.utils.log import get_task_logger

@celery.task(ignore_result=True)
def process_tweet(tweet, send_to_es=True, use_pq=True, debug=True):
    pass


@celery.task(ignore_result=True)
def send_to_s3(tweet):
    pass
