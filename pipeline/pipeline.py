from flask import Flask, request, Blueprint, current_app, jsonify, Response
from flask import current_app as app
from basic_auth import requires_auth_func
import json
from logger import Logger
import os
import json
import pdb
import subprocess

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
    pass


@blueprint.route('/stop', methods=['GET'])
def stop():
    pass

@blueprint.route('/status', methods=['GET'])
def status():
    cmd = "systemctl status logstash | grep Active | awk '{print $2}'"
    return subprocess.check_output([cmd], shell=True).decode().strip()

@blueprint.route('/config', methods=['GET', 'POST'])
def manage_config():
    if request.method == 'GET':
        path = os.path.join(app.root_path, 'pipeline', 'config', 'stream.conf')
        if not os.path.exists(path):
            return Response("No configuration file present", status=500, mimetype='text/plain')
        # load config from file
        with open(path, 'r') as f:
            data = json.load(f)
        return jsonify(data)
    else:
        config = request.get_json(force=True)
        logger.debug("Received configuration: {}".format(config))

        if config is None:
            return Response("Configuration empty", status=400, mimetype='text/plain')

        parser = TreetopParser(app.config)
        required_keys = ['keywords', 'es_index_name', 'lang']
        for d in config:
            if not keys_are_present(required_keys, d):
                logger.error("One or more of the following keywords are not present in the sent configuration: {}".format(required_keys))
                return Response("Invalid configuration", status=400, mimetype='text/plain')

            file_data = parser.create_twitter_input(d['keywords'], d['es_index_name'], d['lang'])
            f_name = d['slug'] + '.conf'
            path = os.path.join(app.config['LOGSTASH_CONFIG_PATH'], f_name)
            with open(path, 'w') as f:
                f.write(file_data)

        return Response("Successfully updated", status=200, mimetype='text/plain')
 

# helpers
def keys_are_present(keys, obj):
    """Test if all keys present"""
    for k in keys:
        if k not in obj:
            return False
    return True


class TreetopParser():
    """Parser for logstash config files in treetop format"""

    def __init__(self, config=None):
        self.config = config


    def create_twitter_input(self, keywords, project, lang):
        data = ""
        data += self.treetop_key_start('input')
        data += self.treetop_key_start('twitter', nesting_level=1)
        data += self.treetop_item('consumer_key', self.config['CONSUMER_KEY'], nesting_level=2)
        data += self.treetop_item('consumer_secret', self.config['CONSUMER_SECRET'], nesting_level=2)
        data += self.treetop_item('oauth_token', self.config['OAUTH_TOKEN'], nesting_level=2)
        data += self.treetop_item('oauth_token_secret', self.config['OAUTH_TOKEN_SECRET'], nesting_level=2)
        data += self.treetop_item('keywords', keywords, nesting_level=2)
        data += self.treetop_item('languages', lang, nesting_level=2)
        data += self.treetop_item('full_tweet', 'true', nesting_level=2, no_quotes=True)
        data += self.treetop_item('ignore_retweets', 'true', nesting_level=2, no_quotes=True)
        data += self.treetop_item('tags', [project, 'en'], nesting_level=2)
        data += self.treetop_key_end(nesting_level=1)
        data += self.treetop_key_end(nesting_level=0)
        return data

    def treetop_key_start(self, key, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{} {}\n".format(indent, key, '{')

    def treetop_key_end(self, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{}\n".format(indent, '}')

    def treetop_item(self, key, val, nesting_level=0, no_quotes=False):
        indent = '  '*nesting_level
        if isinstance(val, str) and not no_quotes:
            return '{}{} => "{}"\n'.format(indent, key, val)
        else:
            return '{}{} => {}\n'.format(indent, key, val)


        
