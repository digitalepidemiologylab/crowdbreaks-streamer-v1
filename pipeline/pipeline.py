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
    path = os.path.join(app.root_path, 'pipeline', 'config', 'stream.conf')
    if not os.path.exists(path):
        return Response("Path to configuration invalid", status=500, mimetype='text/plain')
    if request.method == 'GET':
        # load config from file
        with open(path, 'r') as f:
            data = json.load(f, 'r'))
        return jsonify(data)
    else:
        config = request.get_json()
        logger.debug(config)
        with open(path, 'w') as f:
            data = json.dump(config, f))
        return Response("Successfully updated", status=200, mimetype='text/plain')
