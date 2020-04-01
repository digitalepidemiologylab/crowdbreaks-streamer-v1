import os
import sys
sys.path.append('../web/')
from app.connections.elastic import Elastic
import argparse
import logging
import json

logger = logging.getLogger(__name__)

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
    elif env in ['stg', 'prd']:
        config = get_config()
        ENV = env.upper()
        for env_var in [f'ELASTICSEARCH_HOST_{ENV}', f'ELASTICSEARCH_PORT_{ENV}', f'AWS_ACCESS_KEY_ID_{ENV}', f'AWS_SECRET_ACCESS_KEY_{ENV}', f'AWS_REGION_{ENV}']:
            new_env_var = env_var.split(f'_{ENV}')[0]
            if env_var not in config:
                raise KeyError(f'Key {env_var} must be present in secrets.list file!')
            config[new_env_var] = config[env_var]
    else:
        raise ValueError(f'Invalid environment {env}')
    return Elastic(local_config=config)

def parse_date(date, input_format='%Y-%m-%d %H:%M:%S', output_format='%a %b %d %H:%M:%S %z %Y'):
    if isinstance(date, str) and 'now' in date:
        return date
    try:
        date = datetime.strptime(date, input_format)
    except:
        logger.warning(f'Could not parse date {date}. Fallback to "now"')
        return 'now'
    date = date.replace(tzinfo=timezone.utc)
    date = date.strftime(output_format)
    return date

def build_doc_query(since=None, until=None, missing_field=None, has_field=None, source=(), lang=None):
    query_must_conditions = []
    query_must_not_conditions = []
    query = {}
    if since is not None or until is not None:
        query_must_conditions.append({'range': {'created_at': {'gte': parse_date(since), 'lte': parse_date(until)}}})
    if missing_field is not None:
        query_must_not_conditions.append({'exists': {'field': query_missing_field}})
    if has_field is not None:
        query_must_conditions.append({'exists': {'field': has_field}})
    if lang is not None:
        query_must_conditions.append({'term': {'lang': lang}})
    if len(source) > 0:
        query['_source'] = list(source)
    # add query conditions
    query['query'] = {'bool': {'must': query_must_conditions, 'must_not': query_must_not_conditions}}
    logger.info('Built the following query:\n{}'.format(json.dumps(query, indent=4)))
    return query

def build_agg_query(interval='day', since=None, until=None, missing_field=None, has_field=None, match_phrase=None, lang=None):
    query_must_conditions = []
    query_must_not_conditions = []
    query = {}
    if since is not None or until is not None:
        query_must_conditions.append({'range': {'created_at': {'gte': parse_date(since), 'lte': parse_date(until)}}})
    if missing_field is not None:
        query_must_not_conditions.append({'exists': {'field': query_missing_field}})
    if has_field is not None:
        query_must_conditions.append({'exists': {'field': has_field}})
    if lang is not None:
        query_must_conditions.append({'term': {'lang': lang}})
    if match_phrase is not None: 
        query_must_conditions.append({'match_phrase': {'text': match_phrase}})
    # add query conditions
    query['query'] = {'bool': {'must': query_must_conditions, 'must_not': query_must_not_conditions}}
    query['aggs'] = {'counts': {'date_histogram': {'field': 'created_at', 'interval': interval, 'format': 'yyyy-MM-dd HH:mm:ss'}}}
    query['size'] = 0
    logger.info('Built the following query:\n{}'.format(json.dumps(query, indent=4)))
    return query

def add_doc_query_args(parser):
    parser.add_argument('-s', '--since', default=None, required=False, type=str, help='Since (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('-u', '--until', default=None, required=False, type=str, help='Until (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('--lang', default=None, required=False, type=str, help='Filter for language')
    parser.add_argument('--source', default=[], nargs='+', help='Get only certain fields')
    parser.add_argument('--missing-field', dest='missing_field', default=None, required=False, type=str, help='Query docs with field missing')
    parser.add_argument('--has-field', dest='has_field', default=None, required=False, type=str, help='Query docs with field')
    return parser

def add_agg_query_args(parser):
    parser.add_argument('--interval', dest='interval', default='day', required=False, type=str, help='Aggregation interval')
    parser.add_argument('--lang', default=None, required=False, type=str, help='Filter for language')
    parser.add_argument('-s', '--since', default=None, required=False, type=str, help='Since (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('-u', '--until', default=None, required=False, type=str, help='Until (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('--missing-field', dest='missing_field', default=None, required=False, type=str, help='Query docs with field missing')
    parser.add_argument('--has-field', dest='has_field', default=None, required=False, type=str, help='Query docs with field')
    parser.add_argument('--match-phrase', dest='match_phrase', default=None, required=False, type=str, help='Query docs matching text field')
    return parser

class ArgParseDefault(argparse.ArgumentParser):
    """Simple wrapper which shows defaults in help"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
