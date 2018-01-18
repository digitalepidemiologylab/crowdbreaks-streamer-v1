import elasticsearch
import json
import os
import logging
from datetime import datetime, timezone
from flask import current_app
from flask import _app_ctx_stack as stack


class Elastic():
    """Interaction with Elasticsearch"""

    def __init__(self, app=None, logger=None):
        self.logger = logger
        self.app = app
        self.connection = None # only used outside of application context
        self.config = None
        if self.logger is None:
            # self.logger = Logger.setup('ES')
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
            keys = ['ELASTICSEARCH_HOST', 'ELASTICSEARCH_PORT', 'ELASTICSEARCH_USERNAME', 'ELASTICSEARCH_PASSWORD']
            if self.config is None:
                self.config = {}
            for k in keys:
                if k not in self.config:
                    self.config[k] = os.environ.get(k, None)

        http_auth = (self.config.get('ELASTICSEARCH_USERNAME', None), self.config.get('ELASTICSEARCH_PASSWORD', None))

        if http_auth[0] is None or http_auth[1] is None:
            return elasticsearch.Elasticsearch(["{}:{}".format(self.config['ELASTICSEARCH_HOST'], self.config['ELASTICSEARCH_PORT'])])
        return elasticsearch.Elasticsearch(["{}:{}".format(self.config['ELASTICSEARCH_HOST'], self.config['ELASTICSEARCH_PORT'])], http_auth=http_auth)


    def test_connection(self):
        """test_connection"""
        test = self.es.ping() 
        if test:
            self.logger.info('Successfully connected to Elasticsearch host {}'.format(self.config['ELASTICSEARCH_HOST']))
        else:
            self.logger.error('Connection to Elasticsearch host {} not successful!'.format(self.config['ELASTICSEARCH_HOST']))
        return test


    def index_tweet(self, tweet):
        """Index new tweet in index name given by tweet['project']. Will not re-index already existing doc with same ID.

        :tweet: tweet to index
        """
        self.es.index(index=tweet['project'], id=tweet['id'], doc_type='tweet', body=tweet, op_type='create')
        self.logger.debug('Tweet with id {} sent to project {}'.format(tweet['id'], tweet['project']))


    def put_template(self, template_name='project', filename='project_template.json', template_sub_folder='templates'):
        """Put template to ES
        By default uses <project_root>/elastic_search/templates/project_template.json
        """
        # read template file
        template_path = os.path.join(os.path.dirname(__file__), template_sub_folder, filename)
        with open(template_path, 'r') as f:
            template = json.load(f)
        res = self.es.indices.put_template(template_name, body=template)
        self.logger.info("Template {} added to Elasticsearch".format(template_path))


    def delete_template(self, template_name):
        """Delete template"""
        res = self.es.indices.delete_template(template_name)
        self.logger.info("Template {} successfully deleted".format(template_name))

    def create_index(self, index_name):
        existing_indices = list(self.es.indices.get_alias('*').keys())
        if index_name in existing_indices:
            self.logger.warning("Aborted. Index {} already exists. Delete index first.".format(index_name))
            return False
        res = self.es.indices.create(index_name)
        self.logger.info("Index {} successfully created".format(index_name))
        return True

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

    def get_sentiment_data(self, index_name, value, field='meta.sentiment.sent2vec_v1.label', **options):
        start_date = options.get('start_date', 'now-20y')
        end_date = options.get('end_date', 'now')
        s_date, e_date = self.parse_dates(start_date, end_date)

        body = {'size': 0, 
                'aggs': {'sentiment': {'date_histogram': {
                    'field': 'created_at',
                    'interval': options.get('interval', 'month'),
                    'format': 'yyyy-MM-dd'}}},
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

