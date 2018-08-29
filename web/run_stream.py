import os
from tweepy import OAuthHandler, TweepError
from app.stream.stream_listener import Listener
from app.stream.stream_manager import StreamManager
import time
import logging.config
from app.settings import Config


def main():
    """Here we instantiate the stream manager, listener and connect to the Twitter streaming API."""
    # setting things up...
    logger = logging.getLogger('stream')
    listener = Listener()
    auth = get_auth()

    # wait for a bit before connecting, in case container will be paused
    logger.debug('Streaming container is ready, sleeping for a bit...')
    time.sleep(10)
    time_last_error = None
    error_count = 0
    while True:
        logger.debug('Trying to connect to Twitter API...')
        try: 
            stream = StreamManager(auth, listener)
            stream.start()
        except TweepError as e:
            stream.stop()
            logger.error(e)
            error += 1
            time_new_error = time.time()
            time_last_error = wait_some_time(time_last_error, time_new_error)

            # consider switching auth keys here...
            # send out email...


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


if __name__ == '__main__':
    # logging config
    logging.config.fileConfig('logging.conf')
    main()
