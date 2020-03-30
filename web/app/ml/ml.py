from flask import Blueprint, jsonify, Response, abort, request
from flask import current_app as app
from flask_restful import reqparse
from app.basic_auth import requires_auth_func
from app.ml.sagemaker import Sagemaker
from helpers import success_response, error_response
import json
from app.utils.predict import Predict
import logging

blueprint = Blueprint('ml', __name__)
reqparse = reqparse.RequestParser(bundle_errors=True)
sagemaker = Sagemaker()
logger = logging.getLogger(__name__)

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

@blueprint.route('/delete_model', methods=['POST'])
def delete_model():
    reqparse.add_argument('model_name', type=str, required=True)
    args = reqparse.parse_args()
    resp = sagemaker.delete_model(model_name=args.model_name)
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
        logger.error(resp)
        return error_response(status_code, 'Prediction unsuccessful.', error_type=resp['Error']['Code'])
    prediction = json.loads(resp['Body'].read())
    return jsonify(prediction)

@blueprint.route('/endpoint_config', methods=['POST'])
def endpoint_config():
    reqparse.add_argument('model_endpoint', type=str, required=True)
    args = reqparse.parse_args()
    resp = sagemaker.predict(args.model_endpoint, {'text': 'this is just a test', 'include_run_config': True})
    status_code = resp['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        logger.error(resp)
        return error_response(status_code, 'Getting config unsuccessful.', error_type=resp['Error']['Code'])
    prediction = json.loads(resp['Body'].read())
    config = prediction['run_config']
    return jsonify(config)

@blueprint.route('/endpoint_labels', methods=['POST'])
def endpoint_labels():
    reqparse.add_argument('model_endpoint', type=str, required=True)
    args = reqparse.parse_args()
    resp = sagemaker.predict(args.model_endpoint, {'text': 'this is just a test'})
    status_code = resp['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        logger.error(resp)
        return error_response(status_code, 'Getting endpoint labels unsuccessful.', error_type=resp['Error']['Code'])
    prediction = json.loads(resp['Body'].read())
    labels = prediction['predictions'][0]['labels_fixed']
    label_vals = Predict.labels_to_int(labels)
    label_obj = {'labels': labels}
    if label_vals is not None:
        label_obj = {**label_obj, 'label_vals': label_vals}
    return jsonify(label_obj)
