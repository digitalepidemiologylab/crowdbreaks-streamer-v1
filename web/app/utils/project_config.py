from app.settings import Config
import os
import json
import logging

logger = logging.getLogger(__name__)


class ProjectConfig():
    """Read, write and validate project configs"""

    def __init__(self):
        self.config_path = self._get_config_path()
        self.required_keys = ['keywords', 'es_index_name', 'lang',  'locales', 'slug', 'storage_mode', 'image_storage_mode', 'model_endpoints', 'compile_trending_tweets', 'compile_trending_topics']

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

    def write(self, config):
        config = self._extract_config(config)
        with open(self.config_path, 'w') as f:
            json.dump(config, f, indent=4)

    def get_tracking_info(self, project):
        """Added to all tweets before pushing into S3"""
        config = self.read()
        for stream in config:
            if stream['slug'] == project:
                info = {key: stream[key] for key in ['lang', 'keywords', 'es_index_name']}
                return info

    def get_es_index_names(self, config):
        return [d['es_index_name'] for d in config]

    def is_valid(self, config):
        """Checks incoming new config whether it is valid. Returns boolean (is_valid) and msg pair"""
        if config is None:
            return False, 'Configuration empty'
        for d in config:
            if not self._keys_are_present(d):
                msg = "One or more of the following keywords are not present in the sent configuration: {}".format(self.required_keys)
                return False, msg
            if not self._validate_data_types(d):
                msg = "One or more of the following configurations is of wrong type: {}".format(d)
                return False, msg
        return True, None

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
        is_valid, resp = self.is_valid(config)
        if not is_valid:
            return False, resp
        return True, ''


    # private methods

    def _keys_are_present(self, obj):
        """Test if all keys present"""
        for k in self.required_keys:
            if k not in obj:
                return False
        return True

    def _validate_data_types(self, obj):
        validations = [['keywords', list], ['lang', list], ['es_index_name', str], ['slug', str], ['model_endpoints', list], ['locales', list], ['compile_trending_tweets', bool], ['compile_trending_topics', bool]]
        for key, data_type in validations:
            if not isinstance(obj[key], data_type):
                return False
        return True

    def _extract_config(self, config):
        new_config = []
        for d in config:
            _d = {}
            for k in self.required_keys:
                _d[k] = d[k]
            new_config.append(_d)
        return new_config

    def _get_config_path(self):
        config = Config()
        return os.path.join(config.CONFIG_PATH, config.STREAM_CONFIG_FILE_PATH)
