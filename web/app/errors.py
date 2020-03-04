from flask import Blueprint, jsonify

blueprint = Blueprint('errors', __name__)

@blueprint.app_errorhandler(Exception)
def handle_unexpected_error(error):
    status_code = 500
    response = {
        'success': False,
        'status': status_code,
        'message': 'An unexpected error has occurred.',
        'error': {
            'type': 'UnexpectedException'
        }
    }
    return jsonify(response), status_code
