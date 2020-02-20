import os
import sys
sys.path.append('../web/')
from app.connections.elastic import Elastic


def get_config():
    secrets_file = os.path.join('..', 'secrets.list')
    if not os.path.isfile(secrets_file):
        raise FileNotFoundError('File secrets.list could not be found')
    config = {}
    with open(secrets_file, 'r') as f:
        for line in f:
            if '=' in line:
                split_line = line.split('=')
                if len(split_line) < 2:
                    continue
                elif len(split_line) == 2:
                    key, value = split_line
                    config[key] = value.strip()
                else:
                    config[split_line[0]] = ''.join(split_line[1:]).strip()
    return config

def get_es_client(env='dev'):
    if env == 'dev':
        config = {'ELASTICSEARCH_HOST': 'localhost', 'ELASTICSEARCH_PORT': 9200}
    if env == 'prd':
        config = get_config()
        for env_var in ['ELASTICSEARCH_HOST_PRD', 'ELASTICSEARCH_PORT_PRD', 'AWS_ACCESS_KEY_ID_PRD', 'AWS_SECRET_ACCESS_KEY_PRD', 'AWS_REGION_PRD']:
            new_env_var = env_var.split('_PRD')[0]
            if env_var not in config:
                raise KeyError(f'Key {env_var} must be present in secrets.list file!')
            config[new_env_var] = config[env_var]
    return Elastic(local_config=config)
