from tweepy import Stream
import tweepy
import logging
from app.stream.stream_config_reader import StreamConfigReader

class StreamManager():
    def __init__(self, auth, listener):
        self.logger = logging.getLogger('stream')
        self.stream = Stream(auth=auth, listener=listener, tweet_mode='extended', parser=tweepy.parsers.JSONParser())
        self.stream_config = StreamConfigReader()

    def start(self):
        config = self.stream_config.get_pooled_config()
        self.logger.info('Starting to track for keywords {} in languages {}'.format(config['keywords'], config['lang']))
        self.stream.filter(track=config['keywords'], languages=config['lang'], encoding='utf-8')

    def stop(self):
        self.logger.info('Stopping stream...')
        try:
            self.stream.disconnect()
        except:
            pass
