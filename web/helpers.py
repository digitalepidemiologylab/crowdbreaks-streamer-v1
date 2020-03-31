import rollbar
from app.settings import Config
import pytz
from datetime import datetime
from flask import jsonify
import sys
import gzip
import shutil

def report_error(logger, msg='', level='error', exception=False):
    # exception reporting
    if exception:
        rollbar.report_exc_info(sys.exc_info())
    # logging
    if level == 'warning' and msg != '':
        logger.warning(msg)
    if msg != '':
        logger.error(msg)
        rollbar.report_message(msg, level)

def get_user_tz():
    config = Config()
    try:
        tz = pytz.timezone(config.TIMEZONE)
    except pytz.exceptions.UnknownTimeZoneError:
        tz = pytz.utc
    return tz

def get_tz_difference():
    """Returns time zone difference in hours between time specified by user and UTC."""
    local = datetime.now(get_user_tz())
    utc = local.astimezone(pytz.utc)
    local_utc_replaced = local.replace(tzinfo=pytz.utc) # replace tz in order to be able to compare two UTC times objects
    return utc - local_utc_replaced

def success_response(status_code=200, message=''):
    response = {'success': True, 'message': message}
    return jsonify(response), status_code

def error_response(status_code=400, message='', error_type=''):
    error_obj = {}
    if error_type != '':
        error_obj['type'] = error_type
    response = {
            'success': False,
            'status': status_code,
            'message': message,
            'error': error_obj
            }
    return jsonify(response), status_code

def compress(input_file, output_file):
    with open(input_file, 'rb') as f_in, gzip.open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def decompress(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


