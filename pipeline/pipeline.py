from flask import Flask, request, Blueprint, current_app, jsonify
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
    cmd = "brew services list | grep redis | awk '{print $2}'"
    return subprocess.check_output([cmd], shell=True).decode().strip()

@blueprint.route('/config', methods=['GET', 'POST'])
def manage_config():
    if request.method == 'GET':
        # load config from file
        path = os.path.join(app.root_path, 'pipeline', 'config', 'stream.conf')
        if os.path.exists(path):
            data = json.load(open(path, 'r'))
            return jsonify(data)
        else:
            return jsonify({})
    else:
        config = request.get_json()







