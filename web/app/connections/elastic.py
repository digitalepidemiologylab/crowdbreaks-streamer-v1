import elasticsearch
import json
import os
import logging
from datetime import datetime, timezone
from flask import current_app
from flask import _app_ctx_stack as stack
import glob
import certifi


class Elastic():
    """Interaction with Elasticsearch
    """


    def __init__(self, app=None, logger=None):
        self.logger = logger
        self.app = app
        self.connection = None # only used outside of application context
        self.config = {}
        self.default_template_name = 'project' # default template name when creating new index
        if self.logger is None:
            self.logger = logging.getLogger('ES')
            
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
            self.logger.debug('No app context found!')
            # self.config = { 'ELASTICSEARCH_HOST': os.environ.get('ELASTICSEARCH_HOST', 'localhost'), 
            #         'ELASTICSEARCH_PORT': os.environ.get('ELASTICSEARCH_PORT', 9200) }
            return elasticsearch.Elasticsearch(["{}:{}".format(os.environ.get('ELASTICSEARCH_HOST', 'localhost'), os.environ.get('ELASTICSEARCH_PORT', 9200))])

        # if not self.config['ELASTICSEARCH_HOST'] in ['localhost', 'elasticsearch']:
        #     self.logger.info('Connecting to non-local host {}...'.format(self.config['ELASTICSEARCH_HOST']))
        #     uname, pw = 'ELASTICSEARCH_USERNAME', 'ELASTICSEARCH_PASSWORD'
        #     http_auth = (self.config.get(uname, os.environ.get(uname, None)), self.config.get(pw, os.environ.get(pw, None)))
        return elasticsearch.Elasticsearch(["{}".format(self.config['ELASTICSEARCH_HOST'])], ca_certs=certifi.where(), timeout=30, max_retries=10, retry_on_timeout=True)

        # return elasticsearch.Elasticsearch(["{}:{}".format(self.config['ELASTICSEARCH_HOST'], self.config['ELASTICSEARCH_PORT'])])

    def test_connection(self):
        """test_connection"""
        test = self.es.ping() 
        if test:
            self.logger.info('Successfully connected to Elasticsearch host {}'.format(self.config['ELASTICSEARCH_HOST']))
        else:
            self.logger.error('Connection to Elasticsearch host {} not successful!'.format(self.config['ELASTICSEARCH_HOST']))
        return test


    def cluster_health(self):
        return self.es.cluster.health()


    def index_tweet(self, tweet):
        """Index new tweet in index name given by tweet['project']. Will not re-index already existing doc with same ID.

        :tweet: tweet to index
        """
        self.es.index(index=tweet['project'], id=tweet['id'], doc_type='tweet', body=tweet, op_type='create')
        self.logger.debug('Tweet with id {} sent to project {}'.format(tweet['id'], tweet['project']))


    def put_template(self, filename='project.json', template_path=None):
        """Put template to ES
        """
        # read template file
        template_name = os.path.basename(filename).split('.json')[0]
        if template_path is None:
            template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' ,'config', 'es_templates', filename))
        else:
            template_path = os.path.abspath(os.path.join(template_path, filename))
        if not os.path.exists(template_path):
            self.logger.error('No project file found under {}'.format(template_path))
            return
        with open(template_path, 'r') as f:
            template = json.load(f)
        res = self.es.indices.put_template(template_name, body=template)
        self.logger.info("Template {} added to Elasticsearch".format(template_path))

    def put_mapping(self, index_name, filename):
        mapping_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' ,'config', 'es_mappings', filename))
        with open(mapping_path) as f:
            mapping = json.load(f)
        res = self.es.indices.put_mapping(index=index_name, doc_type='tweet', body=mapping)
        self.logger.info("Mapping {} added to Elasticsearch index {}".format(mapping_path, index_name))

    def list_templates(self):
        templates = self.es.cat.templates(format='json', h=['name'])
        return [t['name'] for t in templates if not t['name'].startswith('.')]

    def delete_template(self, template_name):
        """Delete template"""
        res = self.es.indices.delete_template(template_name)
        self.logger.info("Template {} successfully deleted".format(template_name))

    def create_index(self, index_name, template_name=None):
        # abort if index already exists
        existing_indices = list(self.es.indices.get_alias('*').keys())
        if index_name in existing_indices:
            self.logger.warning("Aborted. Index {} already exists. Delete index first.".format(index_name))
            return False

        # add templates
        self.add_all_templates()

        # create new index
        res = self.es.indices.create(index_name)
        self.logger.info("Index {} successfully created".format(index_name))
        return True

    def add_all_templates(self):
        """Add missing templates from es_templates folder"""
        # add templates
        template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' ,'config', 'es_templates'))
        template_files = glob.glob(os.path.join(template_dir, '*.json'))
        for template_file in template_files:
            template_name = os.path.basename(template_file).split('.json')[0]
            if template_name not in self.list_templates():
                res = self.put_template(filename=template_file)

    def delete_index(self, index_name):
        existing_indices = self.list_indices()
        if index_name not in existing_indices:
            self.logger.warning("Aborted. Index {} doesn't exist".format(index_name))
            return
        res = self.es.indices.delete(index_name)
        self.logger.info("Index {} successfully deleted".format(index_name))

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

    def get_sentiment_data(self, index_name, value, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)
        field = 'meta.sentiment.{}.label'.format(options.get('model', 'fasttext_v1'))
        body = {'size': 0, 
                'aggs': {'sentiment': {'date_histogram': {
                    'field': 'created_at',
                    'interval': options.get('interval', 'month'),
                    'format': 'yyyy-MM-dd HH:mm:ss'}}},
                'query': {'bool': {
                    'must': [
                        {'match_phrase': {field: value}},
                        {'range': {'created_at': {'gte': s_date, 'lte': e_date}}}
                        ]}}
                }
        res = self.es.search(index=index_name, body=body, filter_path=['aggregations.sentiment'])
        if keys_exist(res, 'aggregations', 'sentiment', 'buckets'):
            return res['aggregations']['sentiment']['buckets']
        else:
            return []

    def get_geo_sentiment(self, index_name, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)
        field = 'meta.sentiment.{}.label'.format(options.get('model', 'fasttext_v1'))
        body = {
                'size': options.get('limit', 1000),
                '_source': ['place.average_location', field],
                'query': {
                    'bool': {
                        'must': [
                            {'exists': {'field': 'place.average_location'}},
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


    def get_all_agg(self, index_name, **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)

        body = {'size': 0, 
                'aggs': {'sentiment': {'date_histogram': {
                    'field': 'created_at',
                    'interval': options.get('interval', 'month'),
                    'format': 'yyyy-MM-dd HH:mm:ss' }}},
                'query': { 'range': {'created_at': {'gte': s_date, 'lte': e_date}}}
                }
        res = self.es.search(index=index_name, body=body, filter_path=['aggregations.sentiment'])
        if keys_exist(res, 'aggregations', 'sentiment', 'buckets'):
            return res['aggregations']['sentiment']['buckets']
        else:
            return []


    def get_random_document_id(self, index_name, doc_type='tweet', seed=None):
        if seed is None:
            seed = 42
        # When upgrading to ES 6 'random_score' requires 'field': '_seq_no'
        body = {'query': {'function_score': {'functions': [{'random_score': {'seed': str(seed)}}]}}}
        res =  self.es.search(index=index_name, doc_type=doc_type, body=body, size=1, filter_path=['hits.hits'])
        hits = res['hits']['hits']
        if len(hits) == 0:
            self.logger.error('Could not find a random document in index {}'.format(index_name))
            return None
        return hits[0]['_source']['id']


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
                self.logger.error('Date {} is not of format {}. Using "now" instead'.format(d, input_format))
                res.append('now')
            else:
                d_date = d_date.replace(tzinfo=timezone.utc)
                res.append(d_date.strftime(output_format))
        return res


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

