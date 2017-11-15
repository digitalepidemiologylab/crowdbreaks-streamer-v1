import elasticsearch
import json
import os
import config
import instance.config
from logger import Logger


class Elastic(object):

    """Interaction with Elasticsearch"""

    def __init__(self, es=None, config_path='./../'):
        self.logger = Logger.setup('ES')

        # connect
        if es is None:
            self.es = elasticsearch.Elasticsearch(["{}:{}".format(config.ELASTICSEARCH_HOST, config.ELASTICSEARCH_PORT)],
                    http_auth=(instance.config.ELASTICSEARCH_USERNAME, instance.config.ELASTICSEARCH_PASSWORD))
        else:
            self.es = es

        # test connection
        if self.es.ping():
            self.logger.info('Successfully connected to ElasticSearch')
        else:
            self.logger.error('Connection to ElasticSearch not successful')


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


        





