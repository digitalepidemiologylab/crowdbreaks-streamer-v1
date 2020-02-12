import json
import os
import logging
from flask import Response
from helpers import report_error

class PipelineConfig():
    """Read and write pipeline configuration"""

    def __init__(self, config=None, app_config=None):
        self.app_config = app_config
        self.config = config
        self.logger = logging.getLogger('pipeline')
        self.required_keys = ['keywords', 'es_index_name', 'lang',  'locales', 'slug', 'storage_mode', 'image_storage_mode', 'model_endpoints']

    def write(self):
        config_path = self._get_config_path()
        if config_path is None or self.config is None:
            report_error(self.logger, 'Cannot write pipeline config file.')
            return
        new_config = self._extract_config()
        with open(config_path, 'w') as f:
            json.dump(new_config, f, indent=4)

    def read(self):
        config_path = self._get_config_path()
        if config_path is None or not os.path.isfile(config_path):
            report_error(self.logger, 'Cannot find pipeline config file.', level='warning')
            return []
        with open(config_path, 'r') as f:
            config = json.load(f)
            return config

    def is_valid(self):
        if self.config is None:
            return False, Response("Configuration empty", status=400, mimetype='text/plain')
        for d in self.config:
            if not self._keys_are_present(d):
                msg = "One or more of the following keywords are not present in the sent configuration: {}".format(self.required_keys)
                report_error(self.logger, msg)
                return False, Response("Invalid configuration", status=400, mimetype='text/plain')
            if not self._validate_data_types(d):
                msg = "One or more of the following configurations is of wrong type: {}".format(d)
                report_error(self.logger, msg)
                return False, Response("Invalid configuration", status=400, mimetype='text/plain')
        return True, None

    def get_es_index_names(self):
        return [d['es_index_name'] for d in self.config]

    # private methods
    def _keys_are_present(self, obj):
        """Test if all keys present"""
        for k in self.required_keys:
            if k not in obj:
                return False
        return True

    def _validate_data_types(self, obj):
        validations = [['keywords', list], ['lang', list], ['es_index_name', str], ['slug', str], ['model_endpoints', list]]
        for key, data_type in validations:
            if not isinstance(obj[key], data_type):
                return False
        return True

    def _extract_config(self):
        new_config = []
        for d in self.config:
            _d = {}
            for k in self.required_keys:
                _d[k] = d[k]
            new_config.append(_d)
        return new_config

    def _get_config_path(self):
        if self.app_config is not None:
            return os.path.join(self.app_config['CONFIG_PATH'], self.app_config['STREAM_CONFIG_FILE_PATH'])
        else:
            report_error(self.logger, 'No app config provided. Config path not available.')
            return None
