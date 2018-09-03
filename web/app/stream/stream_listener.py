from app.stream.errors import ERROR_CODES
from app.stream.tasks import handle_tweet
from tweepy import StreamListener
import logging
import json
import time
import re


class Listener(StreamListener):
    """ Handles data received from the stream. """
    def __init__(self):
        super(Listener, self).__init__()
        self.logger = logging.getLogger('stream')
        self.rate_error_count = 0
 
    def on_status(self, status):
        tweet = status._json
        handle_tweet.delay(tweet)
        return True

    def on_error(self, status_code):
        if status_code in ERROR_CODES: 
            self.logger.error('Error {}: {} {}'.format(status_code, ERROR_CODES[status_code]['text'], ERROR_CODES[status_code]['description']))
            if status_code == 420:
                self.logger.error('Waiting for a bit...')
                self.rate_error_count += 1
                # wait at least 15min
                time.sleep(self.rate_error_count*15*60)
            else:
                self.logger.error('Received unknown error code {}'.format(status_code))
        return True # To continue listening

    def on_timeout(self):
        self.logger.error('Timeout...')
        return True # To continue listening

    def on_connect(self):
        self.rate_error_count = 0  # Reset error count
        self.logger.info('Successfully connected to Twitter Streaming API.')

    def on_timeout(self):
        self.logger.info('Streaming connection timed out.')

    def on_warning(self, notice):
        self.logger.warning(notice)
