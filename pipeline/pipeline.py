from flask import Flask, request, Blueprint, jsonify, Response
from flask import current_app as app
from basic_auth import requires_auth_func
import json
from logger import Logger
import os, sys
import json
import pdb
import subprocess
import glob
import ast
import time

blueprint = Blueprint('pipeline', __name__)
logger = Logger.setup('pipeline')

@blueprint.before_request
def require_auth_all():
    requires_auth_func()

@blueprint.route('/', methods=['GET'])
def index():
    return "hello world from pipeline"


@blueprint.route('/start', methods=['GET'])
def start():
    resp =  change_stream_status('start')
    if resp == 'unavailable':
        return Response("Currently not supported on your system.", status=400, mimetype='text/plain')
    if test_for_status('active'):
        return Response("Successfully started stream.", status=200, mimetype='text/plain')
    else:
        return Response("Starting stream was not successful ", status=400, mimetype='text/plain')


@blueprint.route('/stop', methods=['GET'])
def stop():
    resp =  change_stream_status('stop')
    if resp == 'unavailable':
        return Response("Currently not supported on your system.", status=400, mimetype='text/plain')
    if test_for_status('inactive'):
        return Response("Successfully stopped stream.", status=200, mimetype='text/plain')
    else:
        return Response("Stopping stream was not successful ", status=400, mimetype='text/plain')


@blueprint.route('/status', methods=['GET'])
def status():
    resp =  change_stream_status('status')
    return Response(resp, status=200, mimetype='text/plain')


def change_stream_status(mode):
    # only available on linux machines
    if sys.platform in ['linux', 'linux2']:
        cmd = "systemctl {} logstash | grep Active | awk '{print $2}'".format(mode)
        return subprocess.check_output([cmd], shell=True).decode().strip()
    else:
        return 'unavailable'

def test_for_status(status, num_trials=3):
    # check num_trials times if successfully stopped
    for i in xrange(num_trials):
        resp =  change_stream_status('status')
        if resp == status:
            return True
        time.sleep(0.1)
    return False


@blueprint.route('/config', methods=['GET', 'POST'])
def manage_config():
    parser = TreetopParser(config=app.config)
    folder_path = app.config['LOGSTASH_CONFIG_PATH']
    files = glob.glob(folder_path + '/stream-*.conf')
    if request.method == 'GET':
        if not os.path.exists(folder_path):
            return Response("Folder {} not present on remote host.".format(app.config['LOGSTASH_CONFIG_PATH']), status=500, mimetype='text/plain')
        # load config from file
        config_data = {}
        for f in files:
            slug = f.split('/')[-1].split('.conf')[0][7:]
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

        # write new configs
        for d in config:
            file_data = parser.create_twitter_input(d['keywords'], d['es_index_name'], d['lang'])
            f_name = 'stream-' + d['slug'] + '.conf'
            path = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], f_name)
            with open(path, 'w') as f:
                f.write(file_data)

        return Response("Successfully updated configuration files.", status=200, mimetype='text/plain')
 

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
