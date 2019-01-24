import rollbar
from app.settings import Config
import pytz
from datetime import datetime

def report_error(logger, msg, level='error'):
    if level == 'error':
        logger.error(msg)
    elif level == 'warning':
        logger.warning(msg)
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
