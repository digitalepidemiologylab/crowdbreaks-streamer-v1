from app.worker.celery_init import celery
from celery.utils.log import get_task_logger
from app.settings import Config
from app.utils.project_config import ProjectConfig
from app.utils.predict_queue import PredictQueue
from app.utils.priority_queue import TweetStore
from app.utils.data_dump_ids import DataDumpIds
from app.utils.predict import Predict
from app.stream.s3_handler import S3Handler
from app.stream.redis_s3_queue import RedisS3Queue
from app.stream.es_queue import ESQueue
from app.utils.mailer import StreamStatusMailer
from app.extensions import es
from app.stream.trending_tweets import TrendingTweets
from app.stream.trending_topics import TrendingTopics
from helpers import report_error, compress
import logging
import os
import json
import datetime
import uuid

config = Config()

@celery.task(name='s3-upload-task', ignore_result=True)
def send_to_s3(debug=False):
    logger = get_logger(debug)
    s3_handler = S3Handler()
    redis_queue = RedisS3Queue()
    logger.info('Pushing tweets to S3')
    project_keys = redis_queue.find_projects_in_queue()
    project_config = ProjectConfig()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    for key in project_keys:
        project = key.decode().split(':')[-1]
        logger.info('Found {:,} new tweet(s) in project {}'.format(redis_queue.num_elements_in_queue(key), project))
        stream_config = project_config.get_config_by_slug(project)
        now = datetime.datetime.now()
        f_name = 'tweets-{}-{}.jsonl'.format(now.strftime("%Y%m%d%H%M%S"), str(uuid.uuid4()))
        # dump data from redis into a temporary file
        tmp_file_path  = os.path.join(os.path.join('/', 'tmp', f_name))
        with open(tmp_file_path, 'wb') as f:
            for tweets in redis_queue.pop_all_iter(key):
                f.write(b'\n'.join(tweets) + b'\n')
        # compress temporary file
        f_name_gz = f_name + '.gz'
        tmp_file_path_gz  = os.path.join(os.path.join('/', 'tmp', f_name_gz))
        compress(tmp_file_path, tmp_file_path_gz)
        os.remove(tmp_file_path)
        # upload to S3
        s3_key = 'tweets/{}/{}/{}'.format(stream_config['es_index_name'], now.strftime("%Y-%m-%d"), f_name_gz)
        if s3_handler.upload_file(tmp_file_path_gz, s3_key):
            logging.info(f'Successfully uploaded file {s3_key} to S3')
            os.remove(tmp_file_path_gz)
        else:
            logging.error(f'ERROR: Upload of file {s3_key} to S3 not successful')


@celery.task(name='es-bulk-index-task', ignore_result=True)
def es_bulk_index(debug=True):
    logger = get_logger(debug)
    es_queue = ESQueue()
    project_config = ProjectConfig()
    project_keys = es_queue.find_projects_in_queue()
    if len(project_keys) == 0:
        logger.info('No work available. Goodbye!')
        return
    predictions_by_project = {}
    es_actions = []
    for key in project_keys:
        es_queue_objs = es_queue.pop_all(key)
        if len(es_queue_objs) == 0:
            continue
        project = key.decode().split(':')[-1]
        logger.info(f'Found {len(es_queue_objs):,} tweets in queue for project {project}.')
        stream_config = project_config.get_config_by_slug(project)
        # compile actions for bulk indexing
        es_queue_objs = [json.loads(t.decode()) for t in es_queue_objs]
        actions = [
            {'_id': t['id'],
            '_type': 'tweet',
            '_source': t['processed_tweet'],
            '_index': stream_config['es_index_name']
            } for t in es_queue_objs]
        es_actions.extend(actions)
        # compile predictions to be added to prediction queue after indexing
        predictions_by_project[project] = [t['text_for_prediction'] for t in es_queue_objs if 'text_for_prediction' in t]
    # bulk index
    if len(es_actions) > 0:
        success = es.bulk_actions_in_batches(es_actions, batch_size=1000)
        if not success:
            # dump data to disk
            es_queue.dump_to_disk(es_actions, 'es_bulk_indexing_errors')
            return
        # Queue up for prediction
        for project, objs_to_predict in predictions_by_project.items():
            predict_queue = PredictQueue(project)
            predict_queue.multi_push(objs_to_predict)

@celery.task(name='es-predict', ignore_result=True)
def es_predict(debug=True):
    logger = get_logger(debug)
    project_config = ProjectConfig()
    predictions = {}
    for project_config in project_config.read():
        if len(project_config['model_endpoints']) > 0:
            project = project_config['slug']
            predict_queue = PredictQueue(project)
            predict_objs = predict_queue.pop_all()
            if len(predict_objs) == 0:
                logger.info(f'Nothing to predict for project {project}')
            texts = [t['text'] for t in predict_objs]
            ids = [t['id'] for t in predict_objs]
            es_index_name = project_config['es_index_name']
            for question_tag, endpoints_obj in project_config['model_endpoints'].items():
                for endpoint_name, endpoint_info in  endpoints_obj['active'].items():
                    model_type = endpoint_info['model_type']
                    run_name = endpoint_info['run_name']
                    predictor = Predict(endpoint_name, model_type)
                    preds = predictor.predict(texts)
                    for _id, _pred in zip(ids, preds):
                        if es_index_name not in predictions:
                            predictions[es_index_name] = {}
                        if _id not in predictions[es_index_name]:
                            predictions[es_index_name][_id] = {}
                        if question_tag not in predictions[es_index_name][_id]:
                            predictions[es_index_name][_id][question_tag] = {'endpoints': {}}
                        predictions[es_index_name][_id][question_tag]['endpoints'][run_name] = {
                                'label': _pred['labels'][0],
                                'probability': _pred['probabilities'][0]
                                }
                        # if present, add label vals (numeric values of labels)
                        if 'label_vals' in _pred:
                            predictions[es_index_name][_id][question_tag]['endpoints'][run_name]['label_val'] = _pred['label_vals'][0]
                        if endpoints_obj['primary'] == endpoint_name:
                            # current endpoint is primary endpoint
                            predictions[es_index_name][_id][question_tag]['primary_endpoint'] = run_name
                            predictions[es_index_name][_id][question_tag]['primary_label'] = _pred['labels'][0]
                            if 'label_vals' in _pred:
                                predictions[es_index_name][_id][question_tag]['primary_label_val'] = _pred['label_vals'][0]
    if len(predictions) > 0:
        actions = []
        for es_index_name, pred_es_index in predictions.items():
            for _id, pred_obj in pred_es_index.items():
                actions.append({
                    '_id': _id,
                    '_type': 'tweet',
                    '_op_type': 'update',
                    '_index': es_index_name,
                    '_source': {
                        'doc': {
                            'meta': pred_obj
                            }
                        }
                    })
        success = es.bulk_actions_in_batches(actions)
        if not success:
            # dump data to disk
            es_queue = ESQueue()
            es_queue.dump_to_disk(actions, 'es_bulk_update_errors')

@celery.task(name='cleanup', ignore_result=True)
def cleanup(debug=False):
    logger = get_logger(debug)
    # Cleanup (remove old trending tweets from redis)
    project_config = ProjectConfig()
    projects = []
    for project_config in project_config.read():
        projects.append(project_config['slug'])
        if project_config['compile_trending_tweets']:
            tt = TrendingTweets(project_config['slug'])
            tt.cleanup()
    # cleanup tweet store
    ts = TweetStore()
    ts.cleanup(projects)

@celery.task(name='trending-topics-update', ignore_result=True)
def trending_topics_velocity(debug=False):
    logger = get_logger(debug)
    # Cleanup (remove old trending tweets from redis)
    project_config = ProjectConfig()
    for project_config in project_config.read():
        if project_config['compile_trending_topics']:
            tt = TrendingTopics(project_config['slug'])
            tt.update()

# ------------------------------------------
# EMAIL TASKS
@celery.task(name='stream-status-daily', ignore_result=True)
def stream_status_daily(debug=False):
    logger = get_logger(debug)
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'test-email':
        mailer = StreamStatusMailer(status_type='daily')
        body = mailer.get_full_html()
        mailer.send_status(body)
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_counts(older_than=90)

@celery.task(name='stream-status-weekly', ignore_result=True)
def stream_status_weekly(debug=False):
    logger = get_logger(debug)
    if (config.SEND_EMAILS == '1' and config.ENV == 'prd') or config.ENV == 'test-email':
        mailer = StreamStatusMailer(status_type='weekly')
        body = mailer.get_body()
        mailer.send_status(body)
    else:
        logger.info('Not sending emails in this configuration.')
    # clear redis count cache
    redis_queue = RedisS3Queue()
    redis_queue.clear_counts(older_than=90)

# ------------------------------------------
# Public data dumps
@celery.task(name='public-data-dump-ids', ignore_result=True)
def public_data_dump_ids(debug=False):
    logger = get_logger(debug)
    if config.ENV != 'prd':
        logger.info(f'Data dumps are only collected in production environments.')
        return
    project_config = ProjectConfig()
    for project_config in project_config.read():
        if project_config['compile_data_dump_ids']:
            data_dump_ids = DataDumpIds(project_config['slug'])
            data_dump_ids.sync()
            for mode in ['has_place', 'has_coordinates']:
                data_dump_ids = DataDumpIds(project_config['slug'], mode=mode)
                data_dump_ids.sync()

# ------------------------------------------
# Helper functions
def get_logger(debug=False):
    logger = get_task_logger(__name__)
    if debug:
        logger.setLevel(logging.DEBUG)
    return logger
