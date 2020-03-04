from flask import Blueprint, jsonify, Response, abort, request
from flask import current_app as app
from flask_restful import reqparse
from app.basic_auth import requires_auth_func
from app.ml.sagemaker import Sagemaker
from helpers import success_response, error_response
import json

blueprint = Blueprint('ml', __name__)
reqparse = reqparse.RequestParser(bundle_errors=True)
sagemaker = Sagemaker()

@blueprint.before_request
def require_auth_all():
    return requires_auth_func()

@blueprint.route('/list_models', methods=['GET'])
def list_models(include_tags=True):
    models = sagemaker.list_models()
    return jsonify(models)

@blueprint.route('/list_endpoints', methods=['GET'])
def list_endpoints():
    reqparse.add_argument('active', type=bool, required=False, default=False)
    args = reqparse.parse_args()
    endpoints = sagemaker.list_endpoints(active=args.active)
    return jsonify(endpoints)

@blueprint.route('/list_model_endpoints', methods=['GET'])
def list_model_endpoints():
    model_endpoints = sagemaker.list_model_endpoints()
    return jsonify(model_endpoints)

@blueprint.route('/create_endpoint', methods=['POST'])
def create_endpoint():
    reqparse.add_argument('model_name', type=str, required=True)
    args = reqparse.parse_args()
    resp = sagemaker.create_endpoint(endpoint_name=args.model_name)
    return success_response(200, 'Successfully created endpoint.')

@blueprint.route('/delete_endpoint', methods=['POST'])
def delete_endpoint():
    reqparse.add_argument('model_name', type=str, required=True)
    args = reqparse.parse_args()
    resp = sagemaker.delete_endpoint(endpoint_name=args.model_name)
    return success_response(200, 'Successfully created endpoint.')

@blueprint.route('/predict', methods=['POST'])
def predict():
    reqparse.add_argument('text', type=str, required=True)
    reqparse.add_argument('model_endpoint', type=str, required=True)
    reqparse.add_argument('project', type=str, required=False)
    args = reqparse.parse_args()
    resp = sagemaker.predict(args.model_endpoint, {'text': args.text})
    status_code = resp['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        return error_response(status_code, 'Prediction unsuccessful.', error_type=resp['Error']['Code'])
    prediction = json.loads(resp['Body'].read())
    return jsonify(prediction)
