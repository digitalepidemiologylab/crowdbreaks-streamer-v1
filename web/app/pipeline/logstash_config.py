import os
import logging
from app.pipeline.treetop_parser import TreetopParser
from app.pipeline.stream_config import StreamConfig

class LogstashConfig():
    """Read and write logstash input, filter and output files with the help of TreetopParser"""

    def __init__(self, app_config=None):
        self.app_config = app_config
        self.logger = logging.getLogger('pipeline')
        self.parser = TreetopParser(config=app_config)
        self.stream_config = StreamConfig(app_config=app_config)

    def read(self, f_type='input'):
        config_file = self._get_config_path(f_type=f_type)
        if not os.path.isfile(config_file):
            e = "Config file {} couldn't be found".format(config_file)
            self.logger.error(e)
            return
        return self.parser.parse_twitter_input(config_path)

    def write_input(self):
        config_path = self._get_config_path(f_type='input')
        stream_config = self.stream_config.read()
        pooled_keywords = set()
        pooled_lang = set()
        for stream in stream_config:
            pooled_keywords.update(stream['keywords'])
            pooled_lang.update(stream['lang'])
        logstash_input = self.parser.generate_input_file(pooled_keywords, pooled_lang)
        with open(config_path, 'w') as f:
            f.write(logstash_input)


    def write_ouput(self, rewrite=False):
        config_path = self._get_config_path(f_type='output')
        if not os.path.isfile(config_path) or rewrite:
            output_file_data = parser.generate_output_file()
            with open(config_path, 'w') as f:
                f.write(output_file_data)

    # private methods
    def _get_config_path(self, f_type='input'):
        if self.app_config is not None:
            if f_type == 'filter':
                config_file_name = app.config['LOGSTASH_FILTER_FILE']
            elif f_type == 'output':
                config_file_name = app.config['LOGSTASH_OUTPUT_FILE']
            elif f_type == 'input':
                config_file_name = app.config['LOGSTASH_INPUT_FILE']
            else:
                e = "Accepted values for `f_type include 'input', 'filter' or 'output'"
                self.logger.error(e)
                raise Exception(e)
            return os.path.join(app.config['LOGSTASH_CONFIG_PATH'], config_file_name)
        else:
            e = "No app config provided. Config path not available."
            self.logger.error(e)
            raise Exception(e)

