from app.settings import Config
import os
import json


class StreamConfigReader():
    """Read twitter stream configuration while not in app context (e.g. in celery tasks)"""

    def __init__(self):
        self.config_path = self._get_config_path()
        self.required_keys = ['keywords', 'es_index_name', 'lang', 'slug', 'storage_mode', 'image_storage_mode', 'model_endpoints']

    def get_pooled_config(self):
        """Pool all configs to run in single stream"""
        config = self.read()
        res = {'keywords': set(), 'lang': set()}
        for stream in config:
            res['keywords'].update(stream['keywords'])
            res['lang'].update(stream['lang'])
        return res

    def read(self):
        if not os.path.isfile(self.config_path):
            return []
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        return config

    def get_tracking_info(self, project):
        """Added to all tweets before pushing into S3"""
        config = self.read()
        for stream in config:
            if stream['slug'] == project:
                info = {key: stream[key] for key in ['lang', 'keywords', 'es_index_name']}
                return info

    def get_config_by_project(self, project):
        config = self.read()
        for stream in config:
            if stream['slug'] == project:
                return stream

    def validate_streaming_config(self):
        """Validate streaming config before start of stream"""
        if not os.path.isfile(self.config_path):
          return False, 'Configuration file is not present.'
        config = self.read()
        if len(config) == 0:
          return False, 'Configuration contains no streams.'
        for c in config:
            if set(self.required_keys) != set(c.keys()):
                return False, 'Configuration contains invalid keys.'
        return True, ''


    # private methods

    def _get_config_path(self):
        config = Config()
        return os.path.join(config.CONFIG_PATH, config.STREAM_CONFIG_FILE_PATH)
