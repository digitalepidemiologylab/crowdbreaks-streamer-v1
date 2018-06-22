from flask import Blueprint, jsonify, request, Response
from app.basic_auth import requires_auth_func
from app.extensions import es
import logging
import json

blueprint = Blueprint('es_interface', __name__)
logger = logging.getLogger('ES_interface')


@blueprint.before_request
def require_auth_all():
    return requires_auth_func()

@blueprint.route('/test', methods=['GET'])
def test_es():
    return json.dumps(es.test_connection())

@blueprint.route('/', methods=['GET'])
def index():
    return "hello world from elasticsearch interface"

@blueprint.route('/stats', methods=['GET'])
def indices_stats():
    return jsonify(es.indices_stats())

@blueprint.route('/health', methods=['GET'])
def get_health():
    return jsonify(es.cluster_health())


@blueprint.route('/create', methods=['POST'])
def create_index():
    params = request.get_json()
    if es.create_index(params['name']):
        return Response("Index successfully created.", status=200, mimetype='text/plain')
    else:
        return Response("Index creation not successful.", status=400, mimetype='text/plain')

