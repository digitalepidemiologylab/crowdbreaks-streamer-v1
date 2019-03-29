import os, sys
from tweepy import OAuthHandler, TweepError
from app.stream.stream_listener import Listener
from app.stream.stream_manager import StreamManager
import time
import logging.config
from app.settings import Config
import signal
import rollbar
from helpers import report_error
from urllib3.exceptions import ProtocolError

run = True
stream = None

def main():
    """Here we instantiate the stream manager, listener and connect to the Twitter streaming API."""
    global stream
    # setting things up...
    logger = logging.getLogger('stream')
    listener = Listener()
    auth = get_auth()
    # wait for a bit before connecting, in case container will be paused
    logger.debug('Streaming container is ready, sleeping for a bit...')
    time.sleep(10)
    time_last_error = 0
    error_count_last_hour = 0
    while run:
        logger.debug('Trying to connect to Twitter API...')
        stream = StreamManager(auth, listener)
        try: 
            stream.start()
        except KeyboardInterrupt:
            sys.exit()
        except (TweepError, ConnectionError, ConnectionResetError, ProtocolError) as e:
            stream.stop()
            report_error(logger, e)
            error_count_last_hour = update_error_count(error_count_last_hour, time_last_error)
            time_last_error = time.time()
        except Exception as e:
            stream.stop()
            report_error(logger, 'Uncaught stream exception.')
            report_error(logger, e)
            error_count_last_hour = update_error_count(error_count_last_hour, time_last_error)
            time_last_error = time.time()
        if error_count_last_hour > 3:
            report_error(logger, 'Failing to reconnect. Aborting.')
            sys.exit()
        wait_some_time(time_last_error, error_count_last_hour)
    logger.info('Shutting down...')

def handler_stop_signals(signum, frame):
    global run
    global stream
    run = False
    if stream is not None:
        stream.stop()

def update_error_count(error_count, time_last_error):
    if (time.time() - time_last_error) < 3600:
        return error_count + 1
    return 0  # reset to zero

def wait_some_time(time_last_error, error_count_last_hour):
    base_delay = 60
    if error_count_last_hour == 0:
        time.sleep(base_delay)
    else:
        time.sleep(min(base_delay * error_count_last_hour, 1800)) # don't wait longer than 30min

def get_auth():
    config = Config()
    if config.CONSUMER_KEY is None or config.CONSUMER_SECRET is None or config.OAUTH_TOKEN is None or config.OAUTH_TOKEN_SECRET is None:
        raise Exception('Twitter API keys need to be set for streaming to work.')
    auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.OAUTH_TOKEN, config.OAUTH_TOKEN_SECRET)
    return auth

def rollbar_init():
    config = Config()
    if config.ENV == 'prd':
        rollbar.init(config.ROLLBAR_ACCESS_TOKEN, # access token
                    'production', # Environment name
                    root=os.path.dirname(os.path.realpath(__file__)), # server root directory, makes tracebacks prettier
                    allow_logging_basic_config=False)

if __name__ == '__main__':
    # logging config
    logging.config.fileConfig('logging.conf')
    signal.signal(signal.SIGTERM, handler_stop_signals)
    rollbar_init()
    config = Config()
    try:
        main()
    except:
        rollbar.report_exc_info(sys.exc_info())
