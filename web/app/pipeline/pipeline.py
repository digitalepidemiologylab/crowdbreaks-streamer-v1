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
from app.utils.docker_wrapper import DockerWrapper
from app.pipeline.stream_config import StreamConfig
from app.stream.stream_config_reader import StreamConfigReader
from app.extensions import es
from datetime import datetime, timedelta
from app.stream.redis_s3_queue import RedisS3Queue

blueprint = Blueprint('pipeline', __name__)

@blueprint.before_request
def require_auth_all():
    return requires_auth_func()

@blueprint.route('/start', methods=['GET'])
def start():
    d = DockerWrapper()
    stream_container_name = app.config['STREAM_DOCKER_CONTAINER_NAME'] 
    status = d.container_status(stream_container_name)
    if status == 'running':
        return Response("Stream has already started.", status=400, mimetype='text/plain')
    stream_config = StreamConfigReader()
    is_valid, response_invalid = stream_config.validate_streaming_config()
    if not is_valid:
        return Response(response_invalid, status=400, mimetype='text/plain')
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
    stream_config = StreamConfigReader()
    is_valid, response_invalid = stream_config.validate_streaming_config()
    if not is_valid:
        return Response(response_invalid, status=400, mimetype='text/plain')
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

@blueprint.route('/status/stream_activity', methods=['GET'])
def stream_activity():
    es_activity_threshold_min = int(request.args.get('es_activity_threshold_min', 10))
    redis_counts_threshold_hours = int(request.args.get('redis_counts_threshold_hours', 2))
    # elasticsearch counts
    es_count = es.count_recent_documents(since='now-{}m'.format(es_activity_threshold_min))
    # redis counts
    e = datetime.now()
    s = e - timedelta(hours=redis_counts_threshold_hours)
    redis_s3_queue = RedisS3Queue()
    stream_config_reader = StreamConfigReader()
    dates = list(redis_s3_queue.daterange(s, e, hourly=True))
    redis_count = 0
    for stream in stream_config_reader.read():
        for d in dates:
            d, h = d.split(':')
            redis_count = redis_s3_queue.get_counts(stream['slug'], d, h)
    return jsonify({'redis_count': redis_count, 'es_count': es_count})

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
        # read streaming config
        config_data = stream_config.read()
        return jsonify(config_data)
    else:
        # write streaming config
        # make sure new configuration is valid
        is_valid, resp = stream_config.is_valid()
        if not is_valid:
            return resp
        # write everything to config
        stream_config.write()
        # Create new Elasticsearch indices if needed
        es.update_es_indices(stream_config.get_es_index_names())
        return Response("Successfully updated configuration files. Make sure to restart stream for changes to be active.", status=200, mimetype='text/plain')
