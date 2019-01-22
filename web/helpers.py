import rollbar
from app.settings import Config
import pytz
import datetime

def report_error(logger, msg, level='error'):
    if level == 'error':
        logger.error(msg)
    elif level == 'warning':
        logger.warning(msg)
    rollbar.report_message(msg, level)

def convert_tz(dt, from_tz=None, to_tz=None):
    """Convert between timezones"""
    if from_tz is None:
        # get local time zone
        from_tz = get_local_tz()
    if to_tz is None:
        # get time zone specified by user (fallback on UTC)
        to_tz = get_user_tz()
    return dt.replace(tzinfo=from_tz).astimezone(tz=to_tz)

def get_local_tz():
    return datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo

def get_user_tz():
    config = Config()
    try:
        tz = pytz.timezone(config.TIMEZONE)
    except pytz.exceptions.UnknownTimeZoneError:
        tz = pytz.utc
    return tz
