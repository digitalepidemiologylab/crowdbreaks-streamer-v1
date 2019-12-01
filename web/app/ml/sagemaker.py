import redis
from app.settings import Config
import boto3
import logging
from helpers import report_error
from botocore.exceptions import ClientError

class Sagemaker():
    """Interface class to Sagemaker"""

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
        endpoints = self.list_endpoints(active=True)
        endpoints = {e['EndpointName']: e['EndpointArn'] for e in endpoints}
        for i, model in enumerate(models):
            models[i]['HasEndpoint'] = False
            models[i]['EndpointArn'] = ''
            if model['ModelName'] in list(endpoints.keys()):
                models[i]['HasEndpoint'] = True
                models[i]['EndpointArn'] = endpoints[model['ModelName']]
        for i, model in enumerate(models):
            tags = self.list_tags(model['ModelArn'])
            models[i]['Tags'] = tags
        return models

    def list_models(self):
        models = self.paginate(self._client.list_models, 'Models')
        return models

    def list_endpoints(self, active=False):
        endpoints = self.paginate(self._client.list_endpoints, 'Endpoints')
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

    def paginate(self, client_function, key, max_length=100):
        r = client_function()
        if key not in r:
            return []
        resp = r[key]
        while 'NextToken' in r:
            resp.extend(r[key])
            r = client_function(next_token=r['NextToken'])
            if len(resp) > max_length:
                break
        return resp


    # private methods

    @property
    def _client(self):
        return boto3.Session(region_name=self.config.AWS_REGION).client('sagemaker')
