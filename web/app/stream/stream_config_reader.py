from app.settings import Config
import os
import json


class StreamConfigReader():
    """Read twitter stream configuration"""

    def __init__(self):
        self.config_path = self._get_config_path()

    def get_pooled_config(self):
        """Pool all configs to run in single stream"""
        config = self.read()
        res = {'keywords': set(), 'lang': set()}
        for stream in config:
            res['keywords'].update(stream['keywords'])
            res['lang'].update(stream['lang'])
        return res

    def read(self):
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        return config


    # private methods

    def _get_config_path(self):
        config = Config()
        return os.path.join(config.CONFIG_PATH, config.STREAM_CONFIG_FILE_PATH)
