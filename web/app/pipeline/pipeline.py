from flask import Flask, request, Blueprint, jsonify, Response
from flask import current_app as app
from app.basic_auth import requires_auth_func
import json
import os, sys
import json
import pdb
import subprocess
import glob
import time
import logging
from app.connections import elastic
from app.pipeline.docker_wrapper import DockerWrapper
from app.pipeline.stream_config import StreamConfig

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
    stream_container_name = app.config['STREAM_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(stream_container_name)
    if status == 'running':
        return Response("Stream has already started.", status=400, mimetype='text/plain')
    if not stream_config_file_exists():
        return Response("Invalid configuration", status=400, mimetype='text/plain')
    d.unpause_container(stream_container_name)
    status = d.container_status(stream_container_name)
    if status == 'running':
        return Response("Successfully started stream.", status=200, mimetype='text/plain')
    else:
        return Response("Starting stream was not successful ", status=400, mimetype='text/plain')

@blueprint.route('/stop', methods=['GET'])
def stop():
    d = DockerWrapper()
    stream_container_name = app.config['STREAM_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(stream_container_name)
    if status != 'running':
        return Response("Stream has already stopped.", status=400, mimetype='text/plain')
    d.stop_container(stream_container_name)
    d.start_container(stream_container_name)
    d.pause_container(stream_container_name)
    return Response("Successfully stopped stream.", status=200, mimetype='text/plain')


@blueprint.route('/restart', methods=['GET'])
def restart():
    d = DockerWrapper()
    stream_container_name = app.config['STREAM_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(stream_container_name)
    if not stream_config_file_exists():
        return Response("Invalid configuration", status=400, mimetype='text/plain')
    if status != 'running':
        return Response("Can only restart a running stream.", status=400, mimetype='text/plain')
    d.stop_container(stream_container_name)
    d.start_container(stream_container_name)
    status = d.container_status(stream_container_name)
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
    if container_name == 'stream':
        container_name = app.config['STREAM_DOCKER_CONTAINER_NAME']
    try:
        resp = d.container_status(container_name)
    except:
        resp = 'unavailable'
    return Response(resp, status=200, mimetype='text/plain')


@blueprint.route('/config', methods=['GET', 'POST'])
def manage_config():
    logger = logging.getLogger('pipeline')
    stream_config = StreamConfig(config=request.get_json(), app_config=app.config)
    if request.method == 'GET':
        config_data = stream_config.read()
        return jsonify(config_data)
    else:
        # make sure new configuration is valid
        is_valid, resp = stream_config.is_valid()
        if not is_valid:
            return resp
        # write everything to config
        stream_config.write()
        # Create new Elasticsearch index if index doesn't exist already for project
        # es = elastic.Elastic()
        # es_indexes = es.list_indices()
        # for d in config:
        #     if d['es_index_name'] not in es_indexes:
        #         logger.info('Index "{}" does not yet exist in elasticsearch. Creating new index...'.format(d['es_index_name']))
        #         es.create_index(d['es_index_name'])
                
        return Response("Successfully updated configuration files. Make sure to restart stream for changes to be active.", status=200, mimetype='text/plain')
 

# helpers
def stream_config_file_exists():
    config_file_path = os.path.join(app.config['CONFIG_PATH'], app.config['STREAM_CONFIG_FILE_PATH'])
    return os.path.isfile(config_file_path)
