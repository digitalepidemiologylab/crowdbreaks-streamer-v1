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
    time_last_error = None
    error_count = 0
    while run:
        logger.debug('Trying to connect to Twitter API...')
        try: 
            stream = StreamManager(auth, listener)
            stream.start()
        except TweepError as e:
            stream.stop()
            report_error(logger, e)
            error += 1
            time_new_error = time.time()
            time_last_error = wait_some_time(time_last_error, time_new_error)
            # consider switching auth keys here...
        time.sleep(4)
    logger.info('Shutting down...')

def handler_stop_signals(signum, frame):
    global run
    global stream
    run = False
    if stream is not None:
        stream.stop()

def wait_some_time(time_last_error, time_new_error):
    if time_last_error is None:
        time.sleep(5)
        return time_new_error
    time_since_last_error = time_new_error - time_last_error
    if time_since_last_error < 10:
        delay = 20
    elif time_since_last_error < 30:
        delay = 100
    elif time_since_last_error < 200:
        delay = 400
    else:
        delay = 600
    time.sleep(delay)
    return time_new_error

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
