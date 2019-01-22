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
    """Assume dt is in UTC, convert to user timezone"""
    if from_tz is None:
        # get local time zone
        from_tz = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    if to_tz is None:
        # get time zone specified by user (fallback on UTC)
        to_tz = get_user_tz()
    return dt.replace(tzinfo=from_tz).astimezone(tz=to_tz)

def get_user_tz():
    config = Config()
    try:
        tz = pytz.timezone(config.TIMEZONE)
    except pytz.exceptions.UnknownTimeZoneError:
        tz = pytz.utc
    return tz
