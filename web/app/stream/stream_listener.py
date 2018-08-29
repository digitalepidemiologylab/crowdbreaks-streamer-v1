from tweepy import StreamListener
import logging
from app.stream.errors import ERROR_CODES
import json
import time
import re
from app.stream.tasks import process_tweet, send_to_s3


class Listener(StreamListener):
    """ Handles data received from the stream. """
    def __init__(self):
        super(Listener, self).__init__()
        self.logger = logging.getLogger('stream')
        self.rate_error_count = 0
 
    def on_status(self, status):
        # Prints the text of the tweet
        tweet = status._json
        # process_tweet(tweet, send_to_es=False, use_pq=False)
        text = self._get_text(tweet)
        self.logger.debug(text)
        self.logger.info('----------')
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


    # private methods
    def _get_text(self, tweet):
        if 'retweeted_status' in tweet:
            prefix = self._get_retweet_prefix(tweet)
            return prefix + self._get_full_text(tweet['retweeted_status'])
        else:
            return self._get_full_text(tweet)

    def _get_full_text(self, tweet):
        if 'extended_tweet' in tweet:
            return tweet['extended_tweet']['full_text']
        else:
            return tweet['text']

    def _get_retweet_prefix(self, tweet):
        m = re.match(r'^RT (@\w+): ', tweet['text'])
        try:
            return m[0]
        except TypeError:
            return ''
