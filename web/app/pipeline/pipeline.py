from flask import Flask, request, Blueprint, jsonify, Response
from flask import current_app as app
from app.basic_auth import requires_auth_func
import json
import os, sys
import json
import pdb
import subprocess
import glob
import ast
import time
import logging
import docker
from app.connections import elastic

blueprint = Blueprint('pipeline', __name__)

@blueprint.before_request
def require_auth_all():
    return requires_auth_func()


@blueprint.route('/', methods=['GET'])
def index():
    return "hello world from pipeline"


@blueprint.route('/start', methods=['GET'])
def start():
    d = DockerWrapper()
    logstash_container_name = app.config['LOGSTASH_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(logstash_container_name)
    if status == 'running':
        return Response("Stream has already started.", status=400, mimetype='text/plain')
    if not logstash_config_files_exist():
        return Response("Invalid configuration", status=400, mimetype='text/plain')
    d.unpause_container(logstash_container_name)
    status = d.container_status(logstash_container_name)
    if status == 'running':
        return Response("Successfully started stream.", status=200, mimetype='text/plain')
    else:
        return Response("Starting stream was not successful ", status=400, mimetype='text/plain')


@blueprint.route('/stop', methods=['GET'])
def stop():
    d = DockerWrapper()
    logstash_container_name = app.config['LOGSTASH_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(logstash_container_name)
    if status != 'running':
        return Response("Stream has already stopped.", status=400, mimetype='text/plain')
    d = DockerWrapper()
    d.pause_container(logstash_container_name)
    status = d.container_status(logstash_container_name)
    if status != 'running':
        return Response("Successfully stopped stream.", status=200, mimetype='text/plain')
    else:
        return Response("Stopping stream was not successful ", status=400, mimetype='text/plain')


@blueprint.route('/restart', methods=['GET'])
def restart():
    d = DockerWrapper()
    logstash_container_name = app.config['LOGSTASH_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(logstash_container_name)
    
    if not logstash_config_files_exist():
        return Response("Invalid configuration", status=400, mimetype='text/plain')

    d.restart_container(logstash_container_name)
    status = d.container_status(logstash_container_name)
    if status == 'running':
        return Response("Successfully restarted stream.", status=200, mimetype='text/plain')
    else:
        return Response("Restarting stream was not successful ", status=400, mimetype='text/plain')


@blueprint.route('/status/all', methods=['GET'])
def status_all():
    d = DockerWrapper()
    return jsonify(d.list_containers())


@blueprint.route('/status/<container_name>')
def status_container(container_name):
    d = DockerWrapper()
    if container_name == 'logstash':
        container_name = app.config['LOGSTASH_DOCKER_CONTAINER_NAME']
    try:
        resp = d.container_status(container_name)
    except:
        resp = 'unavailable'
    return Response(resp, status=200, mimetype='text/plain')


@blueprint.route('/config', methods=['GET', 'POST'])
def manage_config():
    parser = TreetopParser(config=app.config)
    folder_path = app.config['LOGSTASH_CONFIG_PATH']
    files = glob.glob(os.path.join(folder_path, 'input_stream_*.conf'))
    logger = logging.getLogger('pipeline')
    if not os.path.exists(folder_path):
        return Response("Folder {} not present on remote host.".format(folder_path), status=500, mimetype='text/plain')

    if request.method == 'GET':
        # load config from file
        config_data = {}
        for f in files:
            slug = f.split('/')[-1].split('.conf')[0][len('input_stream_'):]
            parsed_keys = parser.parse_twitter_input(f)
            config_data[slug] = parsed_keys
        return jsonify(config_data)
    else:
        # parse input config
        config = request.get_json()

        if config is None:
            return Response("Configuration empty", status=400, mimetype='text/plain')

        # make sure new configuration is valid
        required_keys = ['keywords', 'es_index_name', 'lang', 'slug']
        for d in config:
            if not keys_are_present(required_keys, d):
                logger.error("One or more of the following keywords are not present in the sent configuration: {}".format(required_keys))
                return Response("Invalid configuration", status=400, mimetype='text/plain')
            if not validate_data_types(d):
                logger.error("One or more of the following configurations is of wrong type: {}".format(d))
                return Response("Invalid configuration", status=400, mimetype='text/plain')

        # delete old configs
        for f in files:
            os.remove(f)

        # write logstash filter/output config file, if not present
        config_path = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], app.config['LOGSTASH_OUTPUT_FILE'])
        if not os.path.isfile(config_path):
            output_file_data = parser.create_output_file()
            try:
                with open(config_path, 'w') as f:
                    f.write(output_file_data)
            except Exception as e:
                logger.error(e)
                return Response("An error occured while writing the file {}".format(config_path), status=400, mimetype='text/plain')

        # write new configs
        for d in config:
            file_data = parser.create_twitter_input(d['keywords'], d['es_index_name'], d['lang'])
            f_name = 'input_stream_' + d['slug'] + '.conf'
            path = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], f_name)
            with open(path, 'w') as f:
                f.write(file_data)

        # Create new Elasticsearch index if index doesn't exist already for project
        es = elastic.Elastic()
        es_indexes = es.list_indices()
        for d in config:
            if d['es_index_name'] not in es_indexes:
                logger.info('Index "{}" does not yet exist in elasticsearch. Creating new index...'.format(d['es_index_name']))
                es.create_index(d['es_index_name'])
                
        return Response("Successfully updated configuration files. Make sure to restart stream for changes to be active.", status=200, mimetype='text/plain')
 

# helpers
def keys_are_present(keys, obj):
    """Test if all keys present"""
    for k in keys:
        if k not in obj:
            return False
    return True

def validate_data_types(obj):
    validations = [['keywords', list], ['lang', list], ['es_index_name', str], ['slug', str]]
    for key, data_type in validations:
        if not isinstance(obj[key], data_type):
            return False
    return True

def logstash_config_files_exist():
    config_path_filter = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], app.config['LOGSTASH_FILTER_FILE'])
    config_path_output = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], app.config['LOGSTASH_OUTPUT_FILE'])
    return os.path.isfile(config_path_filter) and os.path.isfile(config_path_output)


class DockerWrapper():
    """Interaction with docker containers using the docker engine API. This requires access to the docker socket under /var/run/docker.sock.
    For this to work mount a volume containing docker.sock in docker-compose. 
    """
    def __init__(self):
        pass

    @property
    def client(self):
        return docker.from_env()

    def pause_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.pause()

    def start_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.start()

    def unpause_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.unpause()

    def restart_container(self, container_name):
        container = self.client.containers.get(container_name)
        container.restart()

    def list_containers(self):
        return [{'name': c.name, 'status': c.status} for c in self.client.containers.list()]

    def restart_container(self, container_name):
        return self.client.containers.get(container_name).status

    def container_status(self, container_name):
        return self.client.containers.get(container_name).status


class TreetopParser():
    """Parser for logstash config files in treetop format"""

    def __init__(self, config=None):
        self.config = config


    def create_twitter_input(self, keywords, es_index_name, lang):
        data = ""
        data += self.key_start('input')
        data += self.key_start('twitter', nesting_level=1)
        data += self.item('consumer_key', self.config['CONSUMER_KEY'], nesting_level=2)
        data += self.item('consumer_secret', self.config['CONSUMER_SECRET'], nesting_level=2)
        data += self.item('oauth_token', self.config['OAUTH_TOKEN'], nesting_level=2)
        data += self.item('oauth_token_secret', self.config['OAUTH_TOKEN_SECRET'], nesting_level=2)
        data += self.item('keywords', keywords, nesting_level=2)
        data += self.item('languages', lang, nesting_level=2)
        data += self.item('full_tweet', 'true', nesting_level=2, no_quotes=True)
        data += self.item('ignore_retweets', 'true', nesting_level=2, no_quotes=True)
        data += self.item('tags', [es_index_name], nesting_level=2)
        data += self.key_end(nesting_level=1)
        data += self.key_end(nesting_level=0)
        return data

    def key_start(self, key, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{} {}\n".format(indent, key, '{')

    def key_end(self, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{}\n".format(indent, '}')

    def item(self, key, val, nesting_level=0, no_quotes=False):
        indent = '  '*nesting_level
        if isinstance(val, str) and not no_quotes:
            return '{}{} => "{}"\n'.format(indent, key, val)
        else:
            return '{}{} => {}\n'.format(indent, key, val)

    def parse_twitter_input(self, f_name):
        """Parser for twitter input files"""
        res = {}
        fields_to_parse = ['keywords', 'languages', 'tags']
        f = open(f_name, 'r')
        for l in f.readlines():
            if not '=>' in l:
                continue
            key, val = l.split('=>')
            key = key.strip()
            val = val.strip()

            if key in fields_to_parse:
                # parse string to list
                if key == 'tags':
                    res['es_index_name'] = ast.literal_eval(val)[0]
                else:
                    res[key] = ast.literal_eval(val)

        f.close()
        return res

    def create_output_file(self, outputs=['redis']):
        data = ""
        # start output
        data += self.key_start('output')

        # redis output
        data += self.key_start('if "redis" in [type]', nesting_level=1)
        data += self.key_start('redis', nesting_level=2)
        data += self.item('id', 'main-output-plugin', nesting_level=3)
        data += self.item('host', self.config['REDIS_HOST'], nesting_level=3)
        data += self.item('port', self.config['REDIS_PORT'], nesting_level=3, no_quotes=True)
        data += self.item('db', '0', nesting_level=3, no_quotes=True)
        data += self.item('data_type', 'list', nesting_level=3)
        data += self.item('codec', 'json', nesting_level=3)
        data += self.item('key', '{}:{}'.format(self.config['REDIS_NAMESPACE'], self.config['REDIS_LOGSTASH_QUEUE_KEY']), nesting_level=3)
        data += self.key_end(nesting_level=2)
        data += self.key_end(nesting_level=1)

	# s3 output
        if not (self.config['AWS_ACCESS_KEY_ID'] == '' or self.config['AWS_SECRET_ACCESS_KEY'] == '') and 'S3_BUCKET' in self.config:
            data += self.key_start('else', nesting_level=1)
            data += self.key_start('s3', nesting_level=2)
            data += self.item('access_key_id', self.config['AWS_ACCESS_KEY_ID'], nesting_level=3)
            data += self.item('secret_access_key', self.config['AWS_SECRET_ACCESS_KEY'], nesting_level=3)
            data += self.item('region', self.config['AWS_REGION'], nesting_level=3)
            data += self.item('bucket', self.config['S3_BUCKET'], nesting_level=3)
            data += self.item('prefix', 'tweets/%{project}/', nesting_level=3)
            data += self.item('time_file', '1440', nesting_level=3, no_quotes=True)
            data += self.item('codec', 'json_lines', nesting_level=3)
            data += self.key_end(nesting_level=2)
            data += self.key_end(nesting_level=1)

        # std output
        # data += self.key_start('stdout', nesting_level=1)
        # data += self.item('codec', 'line { format => "Collected tweet for project %{project}" }', nesting_level=2, no_quotes=True)
        # data += self.key_end(nesting_level=1)

	# end output
        data += self.key_end(nesting_level=0)
        return data
