import redis
from app.settings import Config
import boto3
import logging
from helpers import report_error
from botocore.exceptions import ClientError
import json
from flask import jsonify

class Sagemaker():
    """Interface class to Sagemaker.
    Note that we are naming models and corresponding endpoints the same. Endpoint configs are named "{model_name/endpoint_name}-config"
    """

    def __init__(self):
        self.config = Config()
        self.bucket = self.config.S3_BUCKET_SAGEMAKER
        self.logger = logging.getLogger(__name__)

    def ping(self):
        try:
            client = self.list_endpoints()
        except ClientError:
            return False
        return True

    def list_model_endpoints(self):
        models = self.list_models()
        endpoints = self.list_endpoints()
        endpoints = {e['EndpointName']: [e['EndpointArn'], e['EndpointStatus']] for e in endpoints}
        for i, model in enumerate(models):
            models[i]['HasEndpoint'] = False
            models[i]['EndpointArn'] = ''
            if model['ModelName'] in list(endpoints.keys()):
                models[i]['HasEndpoint'] = True
                models[i]['EndpointArn'] = endpoints[model['ModelName']][0]
                models[i]['EndpointStatus'] = endpoints[model['ModelName']][1]
        for i, model in enumerate(models):
            tags = self.list_tags(model['ModelArn'])
            models[i]['Tags'] = tags
        return models


    def list_models(self):
        models = self._paginate(self._client.list_models, 'Models')
        return models

    def create_endpoint(self, endpoint_name):
        config_name = f'{endpoint_name}-config'
        tags = [{'Key': 'project', 'Value': 'crowdbreaks'}]
        self._client.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=config_name, Tags=tags)

    def delete_endpoint(self, endpoint_name):
        self._client.delete_endpoint(EndpointName=endpoint_name)

    def delete_model(self, model_name):
        self._client.delete_model(ModelName=model_name)
        self.delete_endpoint_configuration(f'{model_name}-config')

    def delete_endpoint_configuration(self, endpoint_config_name):
        self._client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)

    def list_endpoints(self, active=False):
        endpoints = self._paginate(self._client.list_endpoints, 'Endpoints')
        if active:
            for e in endpoints:
                if e['EndpointStatus'] != 'InService':
                    endpoints.remove(e)
        return endpoints

    def list_tags(self, resource_arn):
        tags = self._client.list_tags(ResourceArn=resource_arn)
        _tags = {}
        for tag in tags['Tags']:
            _tags[tag['Key']] = tag['Value']
        return _tags

    def predict(self, endpoint_name, body):
        return self._runtime_client.invoke_endpoint(EndpointName=endpoint_name, Body=json.dumps(body), ContentType='application/json')

    # private methods

    def _paginate(self, client_function, key, max_length=100):
        r = client_function()
        if key not in r:
            return []
        resp = r[key]
        while 'NextToken' in r:
            r = client_function(NextToken=r['NextToken'])
            resp.extend(r[key])
            if len(resp) > max_length:
                break
        return resp


    @property
    def _client(self):
        return boto3.Session(region_name=self.config.AWS_REGION).client('sagemaker')

    @property
    def _runtime_client(self):
        return boto3.Session(region_name=self.config.AWS_REGION).client('runtime.sagemaker')
