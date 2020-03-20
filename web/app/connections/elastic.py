import elasticsearch
from elasticsearch import helpers as es_helpers
import json
import os
import logging
from datetime import datetime, timezone
from flask import current_app
from flask import _app_ctx_stack as stack
import glob
from aws_requests_auth.aws_auth import AWSRequestsAuth
from helpers import report_error

logger = logging.getLogger(__name__)

class Elastic():
    """Interaction with Elasticsearch
    """

    def __init__(self, app=None, logger=None, local_config=None):
        self.app = app
        self.connection = None # only used outside of application context
        self.config = {}
        self.local_config = local_config
        self.default_template_name = 'project' # default template name when creating new index
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)
        else:
            app.teardown_request(self.teardown)

    def teardown(self, exception):
        ctx = stack.top
        if hasattr(ctx, 'elasticsearch'):
            ctx.elasticsearch = None

    @property
    def es(self):
        ctx = stack.top
        if ctx is not None:
            if not hasattr(ctx, 'elasticsearch'):
                ctx.elasticsearch = self._connect()
            return ctx.elasticsearch
        else:
            # Running outside of application context (e.g. by using a script)
            if self.connection is None:
                self.connection = self._connect()
            return self.connection

    def _connect(self):
        try:
            self.config = current_app.config
        except RuntimeError:
            logger.debug('No app context found!')
            if self.local_config is not None:
                logger.info('Using local configuration')
                _config = self.local_config
            else:
                _config = os.environ
            self.config['ELASTICSEARCH_HOST'] = _config.get('ELASTICSEARCH_HOST', 'localhost')
            self.config['ELASTICSEARCH_PORT'] = _config.get('ELASTICSEARCH_PORT', 9200)
            if  self.config['ELASTICSEARCH_HOST'] in ['localhost', 'elasticsearch']:
                # Access Elasticsearch locally
                return elasticsearch.Elasticsearch(["{}:{}".format(self.config['ELASTICSEARCH_HOST'], self.config['ELASTICSEARCH_PORT'])])
            self.config['AWS_ACCESS_KEY_ID'] = _config.get('AWS_ACCESS_KEY_ID')
            self.config['AWS_SECRET_ACCESS_KEY'] = _config.get('AWS_SECRET_ACCESS_KEY')
            self.config['AWS_REGION'] = _config.get('AWS_REGION')
        auth = AWSRequestsAuth(aws_access_key=self.config['AWS_ACCESS_KEY_ID'], aws_secret_access_key=self.config['AWS_SECRET_ACCESS_KEY'],
                aws_host=self.config['ELASTICSEARCH_HOST'], aws_region=self.config['AWS_REGION'], aws_service='es')
        return elasticsearch.Elasticsearch(host=self.config['ELASTICSEARCH_HOST'], port=int(self.config['ELASTICSEARCH_PORT']),
                connection_class=elasticsearch.RequestsHttpConnection,
                http_auth=auth)

    def test_connection(self):
        """test_connection"""
        test = self.es.ping()
        if test:
            logger.info('Successfully connected to Elasticsearch host {}'.format(self.config['ELASTICSEARCH_HOST']))
        else:
            report_error(logger, msg='Connection to Elasticsearch host {} not successful!'.format(self.config['ELASTICSEARCH_HOST']))
        return test

    def cluster_health(self):
        return self.es.cluster.health()

    def index_tweet(self, tweet, index_name):
        """Index new tweet in index name given by tweet['project']. Will not re-index already existing doc with same ID.

        :tweet: tweet to index
        """
        try:
            self.es.index(index=index_name, id=tweet['id'], doc_type='tweet', body=tweet, op_type='create')
        except elasticsearch.ConflictError as e:
            # This usually happens when a document with the same ID already exists.
            logger.warning('Conflict Error')
        except elasticsearch.TransportError as e:
            report_error(logger, exception=True)
        else:
            logger.debug('Tweet with id {} sent to index {}'.format(tweet['id'], index_name))

    def bulk_actions_in_batches(self, actions, batch_size=1000):
        num_actions = len(actions)
        logger.info(f'Processing {num_actions:,} bulk actions...')
        for i in range(0, num_actions, batch_size):
            num_actions_in_batch = len(actions[i:(i+batch_size)])
            try:
                self.bulk_action(actions[i:(i+batch_size)])
            except:
                logger.error(f'Elasticsearch failed to process batch of {num_actions_in_batch:,} actions')
                report_error(logger, exception=True)
                return False
            else:
                logger.info(f'Successfully processed batch of {num_actions_in_batch:,} actions')
        return True

    def bulk_action(self, actions):
        logger.info('Bulk operation...')
        es_helpers.bulk(self.es, actions, timeout='60s')

    def put_template(self, template_path, template_name):
        """Put template to ES
        """
        # read template file
        if not os.path.exists(template_path):
            report_error(logger, msg='No project file found under {}'.format(template_path))
            return
        with open(template_path, 'r') as f:
            template = json.load(f)
        res = self.es.indices.put_template(template_name, body=template, include_type_name=True)
        logger.info("Template {} added to Elasticsearch".format(template_path))

    def put_mapping(self, index_name, filename):
        mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' ,'config', 'es_mappings', filename))
        with open(mapping_path) as f:
            mapping = json.load(f)
        res = self.es.indices.put_mapping(index=index_name, doc_type='tweet', body=mapping)
        logger.info("Mapping {} added to Elasticsearch index {}".format(mapping_path, index_name))

    def list_templates(self):
        templates = self.es.cat.templates(format='json', h=['name'])
        return [t['name'] for t in templates if not t['name'].startswith('.')]

    def delete_template(self, template_name):
        """Delete template"""
        res = self.es.indices.delete_template(template_name)
        logger.info("Template {} successfully deleted".format(template_name))

    def create_index(self, index_name, settings={}, mapping={}):
        # abort if index already exists
        existing_indices = list(self.es.indices.get_alias('*').keys())
        if index_name in existing_indices:
            logger.warning("Aborted. Index {} already exists. Delete index first.".format(index_name))
            return False
        # add templates
        self.add_all_templates()
        # get setting/mapping
        body = {'settings': {'index': settings}, 'mapping': mapping}
        # create new index
        res = self.es.indices.create(index=index_name, body=body, include_type_name=True)
        logger.info("Index {} successfully created".format(index_name))
        return True

    def update_es_indices(self, indices):
        current_indices = self.list_indices()
        for idx in indices:
            if idx not in current_indices:
                self.create_index(idx)

    def add_all_templates(self):
        """Add missing templates from es_templates folder"""
        # add templates
        template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' ,'config', 'es_templates'))
        template_files = glob.glob(os.path.join(template_dir, '*.json'))
        for template_file in template_files:
            template_name = os.path.basename(template_file).split('.json')[0]
            res = self.put_template(template_file, template_name)

    def delete_index(self, index_name):
        existing_indices = self.list_indices()
        if index_name not in existing_indices:
            logger.warning("Aborted. Index {} doesn't exist".format(index_name))
            return
        res = self.es.indices.delete(index_name)
        logger.info("Index {} successfully deleted".format(index_name))

    def indices_stats(self):
        return self.es.indices.stats(filter_path=['indices'])

    def list_indices(self):
        return list(self.es.indices.get_alias('*').keys())

    def delete_field_from_doc(self, index, doc_type, id, field, field_path=None):
        if field_path is None:
            path = 'ctx._source'
        else:
            path = 'ctx._source.{}'.format(field_path)
        body = {'script': "{}.remove(\"{}\")".format(path, field)}
        resp = self.es.update(index=index, doc_type=doc_type, id=id, body=body)
        return resp

    def refresh(self):
        """Refreshes all indices, making new documents visible to search"""
        self.es.indices.refresh()

     #################################################################
     # Trending tweets/topics
    def get_matching_ids_for_query(self, index_name, query, ids, size=10):
        body =  {'query': {'bool': {'must': [{'match_phrase': {'text': query}}, {'ids': {'values': ids}}]}}}
        body['_source'] = False
        res = self.es.search(index=index_name, body=body, size=size)
        res = [hit['_id'] for hit in res['hits']['hits']]
        return res

    def get_trending_topics(self, index_name, interval='hour', s_date='now-1d', e_date='now', top_n=4000, field='counts', with_moving_average=True, moving_average_window=5):
        # build query
        if with_moving_average:
            moving_average = {'moving_average': {'moving_avg': {'buckets_path': 'average_count', 'window': moving_average_window}}}
        else:
            moving_average = {}
        average_count_agg = { "aggs": { "average_count": { "avg": { "field": field } }, **moving_average } }
        average_count_by_interval =  { "aggs": { "sum_counts": { "sum": { "field": "counts" } }, "average_counts_by_interval": { "date_histogram": { "field": "bucket_time", "interval": interval }, **average_count_agg}}}
        full_query = { "aggs": { "by_term": { "terms": { "field": "term", "size": top_n, "order": { "sum_counts": "desc" } }, **average_count_by_interval } } }
        query = {'range': {'bucket_time': {'gte': s_date, 'lte': e_date}}}
        full_query['query'] = query
        # run query
        res = self.es.search(index=index_name, body=full_query, filter_path=['aggregations'])
        try:
            res = res['aggregations']['by_term']['buckets']
        except KeyError:
            return []
        else:
            data = []
            for item in res:
                key = item['key']
                try:
                    time_buckets = item['average_counts_by_interval']['buckets']
                except:
                    continue
                for time_bucket in time_buckets:
                    # parse UTC time
                    time = datetime.strptime(time_bucket['key_as_string'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    value = time_bucket['average_count']['value']
                    moving_average = {}
                    if with_moving_average:
                        try:
                            moving_average = {'moving_average': time_bucket['moving_average']['value']}
                        except:
                            moving_average = {'moving_average': None}
                    data.append({'term': key, 'bucket_time': time, 'value': value, **moving_average})
            return data


    #################################################################
    # Sentiment data

    def get_av_sentiment(self, index_name, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)
        include_retweets = options.get('include_retweets', False)
        # Time range condition
        query_conditions = [{'range': {'created_at': {'gte': s_date, 'lte': e_date}}}]
        # Include retweets condition
        if include_retweets:
            query_conditions.append({'exists': {'field': 'is_retweet'}}) # needs to have is_retweet field
        else:
            exclude_retweets_query = [
                    {'bool': {'must_not': [{'exists': {'field': 'is_retweet'}}]}}, # if field does not exist it is not a retweet, OR ...
                    {'bool': {'must': [{'exists': {'field': 'is_retweet'}}, {'term': {'is_retweet': False}}]}} # if is_retweet field exists it has to be False
                ]
            query_conditions.append({'bool': {'should': exclude_retweets_query}}) # needs to exclude retweets condition

        # full query
        field = 'meta.sentiment.{}'.format(options.get('model', 'fasttext_v1'))
        body = {'size': 0,
                'aggs': {
                    'avg_sentiment': {
                        'date_histogram': {
                            'field': 'created_at',
                            'interval': options.get('interval', 'month'),
                            'format': 'yyyy-MM-dd HH:mm:ss'
                            },
                        'aggs': {
                            'avg_sentiment': {
                                'avg': {
                                    'field': '{}.label_val'.format(field)
                                    }
                                }
                            }
                        }
                    },
                'query': {'bool': {'must': query_conditions}}
                }
        res = self.es.search(index=index_name, body=body, filter_path=['aggregations.avg_sentiment'])
        if keys_exist(res, 'aggregations', 'avg_sentiment', 'buckets'):
            return res['aggregations']['avg_sentiment']['buckets']
        else:
            return []


    def get_geo_sentiment(self, index_name, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)
        field = 'meta.sentiment.{}.label_val'.format(options.get('model', 'fasttext_v1'))
        body = {
                'size': options.get('limit', 10000),
                '_source': ['place.average_location', field],
                'query': {
                    'bool': {
                        'must': [
                            {'exists': {'field': 'place.average_location'}},
                            {'exists': {'field': field}},
                            {'range': {'created_at': {'gte': s_date, 'lte': e_date}}}
                            ]
                        }
                    }
                }
        res = self.es.search(index=index_name, body=body, filter_path=['hits.hits._source'])
        if keys_exist(res, 'hits', 'hits'):
            return res['hits']['hits']
        else:
            return []

    #################################################################
    # Data
    def get_predictions(self, index_name, question_tag, answer_tags, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        end_date = 'now'
        include_retweets = options.get('include_retweets', True)
        s_date, e_date = self.parse_dates(start_date, end_date)
        # Time range condition
        query_conditions = [{'range': {'created_at': {'gte': s_date, 'lte': e_date}}}]
        # for certain old data we don't have retweet information, exclude those
        query_conditions.append({'exists': {'field': 'is_retweet'}})
        # Include retweets condition
        if not include_retweets:
            query_conditions.append({'field': {'is_retweet': False}})
        predictions = {}
        field = f'meta.{question_tag}.primary_label'
        query_conditions.append({'match_phrase': {field: ''}})
        for answer_tag in answer_tags:
            query_conditions[-1]['match_phrase'][field] = answer_tag
            # full query
            body = {'aggs': {
                        'prediction_agg': {
                            'date_histogram': {
                                'field': 'created_at',
                                'interval': options.get('interval', 'month'),
                                'format': 'yyyy-MM-dd HH:mm:ss'
                                }
                            }
                        },
                    'query': {'bool': {'must': query_conditions}}
                    }
            res = self.es.search(index=index_name, body=body, filter_path=['aggregations.prediction_agg'])
            if keys_exist(res, 'aggregations', 'prediction_agg', 'buckets'):
                predictions[answer_tag] = res['aggregations']['prediction_agg']['buckets']
            else:
                predictions[answer_tag] = []
        return predictions

    def get_all_agg(self, index_name, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        keywords = options.get('keywords', [])
        not_keywords = options.get('not_keywords', [])
        s_date, e_date = self.parse_dates(start_date, end_date)
        # query without keywords (only condition on s_date and e_date)
        query = {'bool': {'must': [{'range': {'created_at': {'gte': s_date, 'lte': e_date}}}], 'must_not': []}}
        # add a match_phrase condition for every keyword and not_keyword
        if len(keywords) > 0:
            for keyword in keywords:
                query['bool']['must'].append({'match_phrase': {'text': keyword}})
        if len(not_keywords) > 0:
            for keyword in not_keywords:
                query['bool']['must_not'].append({'match_phrase': {'text': keyword}})
        body = {'size': 0,
                'aggs': {
                    'sentiment': {
                        'date_histogram': {
                            'field': 'created_at',
                            'interval': options.get('interval', 'month'),
                            'format': 'yyyy-MM-dd HH:mm:ss' }
                        }
                    },
                'query': query
                }
        res = self.es.search(index=index_name, body=body, filter_path=['aggregations.sentiment'])
        if keys_exist(res, 'aggregations', 'sentiment', 'buckets'):
            return res['aggregations']['sentiment']['buckets']
        else:
            return []


    #################################################################
    # Misc
    def get_random_document(self, index_name, doc_type='tweet'):
        body = {'query': {'function_score': {'functions': [{'random_score': {}}]}}}
        res =  self.es.search(index=index_name, doc_type=doc_type, body=body, size=1, filter_path=['hits.hits'])
        hits = res['hits']['hits']
        if len(hits) == 0:
            report_error(logger, msg='Could not find a random document in index {}'.format(index_name))
            return None
        return hits[0]['_source']


    def parse_dates(self, *dates, input_format='%Y-%m-%d %H:%M:%S', output_format='%a %b %d %H:%M:%S %z %Y'):
        """Used to parse for Twitter's unusual created_at date format"""
        res = []
        for d in dates:
            if isinstance(d, str) and 'now' in d:
                res.append(d)
                continue
            try:
                d_date = datetime.strptime(d, input_format)
            except:
                report_error(logger, msg='Date {} is not of format {}. Using "now" instead'.format(d, input_format))
                res.append('now')
            else:
                d_date = d_date.replace(tzinfo=timezone.utc)
                res.append(d_date.strftime(output_format))
        return res

    def count_recent_documents(self, since='now-10m'):
        indices = self.list_indices()
        body = {'query': {'range': {'created_at': {'gte': since, 'lte': 'now'}}}}
        resp = self.es.count(index=indices, doc_type='tweet', body=body)
        return resp.get('count', 0)

# Helper functions
def keys_exist(element, *keys):
    """ Check if *keys (nested) exists in `element` (dict). """
    _element = element
    for key in keys:
        try:
            _element = _element[key]
        except KeyError:
            return False
    return True

