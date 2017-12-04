import elasticsearch
import json
import os
import config
import instance.config
from logger import Logger
from datetime import datetime, timezone


class Elastic(object):

    """Interaction with Elasticsearch"""

    def __init__(self, es=None, config_path='./../'):
        self.logger = Logger.setup('ES', use_elasticsearch_logger=False)

        # connect
        if es is None:
            if 'ELASTICSEARCH_PASSWORD' in instance.config.__dict__:
                self.es = elasticsearch.Elasticsearch(["{}:{}".format(instance.config.ELASTICSEARCH_HOST, instance.config.ELASTICSEARCH_PORT)], 
                        http_auth=(instance.config.ELASTICSEARCH_USERNAME, instance.config.ELASTICSEARCH_PASSWORD))
            else:
                self.es = elasticsearch.Elasticsearch(["{}:{}".format(instance.config.ELASTICSEARCH_HOST, instance.config.ELASTICSEARCH_PORT)])
        else:
            self.es = es

        # test connection
        if self.test_connection():
            self.logger.info('Successfully connected to ElasticSearch host {}'.format(instance.config.ELASTICSEARCH_HOST) )
        else:
            self.logger.error('Connection to ElasticSearch host {} not successful'.format(instance.config.ELASTICSEARCH_HOST))


    def test_connection(self):
        return self.es.ping()


    def index_tweet(self, tweet):
        """Index new tweet in index name given by tweet['project']. Will not re-index already existing doc with same ID.

        :tweet: tweet to index
        """

        self.es.index(index='project_'+tweet['project'], id=tweet['id'], doc_type='tweet', body=tweet, op_type='create')
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
            return
        res = self.es.indices.create(index_name)
        self.logger.info("Index {} successfully created".format(index_name))

    def delete_index(self, index_name):
        existing_indices = list(self.es.indices.get_alias('*').keys())
        if index_name not in existing_indices:
            self.logger.warning("Aborted. Index {} doesn't exist".format(index_name))
            return
        res = self.es.indices.delete(index_name)
        self.logger.info("Index {} successfully deleted".format(index_name))

    def get_sentiment_data(self, index_name, value, field='meta.sentiment.sent2vec_v1.0.label', **options):
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
                    'format': 'yyyy-MM-dd' }}},
                'query': { 'range': {'created_at': {'gte': s_date, 'lte': e_date}}}
                }
        res = self.es.search(index=index_name, body=body, filter_path=['aggregations.sentiment'])
        if keys_exist(res, 'aggregations', 'sentiment', 'buckets'):
            return res['aggregations']['sentiment']['buckets']
        else:
            return []


    def parse_dates(self, *dates, input_format='%Y-%m-%d', output_format='%a %b %d %H:%M:%S %z %Y'):
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

