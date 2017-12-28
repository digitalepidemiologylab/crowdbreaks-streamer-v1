from flask import Blueprint, jsonify
from basic_auth import requires_auth_func
from logger import Logger
from extensions import es

blueprint = Blueprint('es_interface', __name__)
logger = Logger.setup('ES interface')


@blueprint.before_request
def require_auth_all():
    return requires_auth_func()

@blueprint.route('/', methods=['GET'])
def index():
    return "hello world from elasticsearch interface"

@blueprint.route('/stats', methods=['GET'])
def es_stats():
    return jsonify(es.stats())

@blueprint.route('/create/<index>', methods=['GET'])
def create_index(index):
    return jsonify(es.stats())
