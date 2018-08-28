import ast
import json
import os

class TreetopParser():
    """Parser for logstash config files in treetop format"""

    def __init__(self, config=None):
        self.config = config

    def generate_input_file(self, keywords, lang):
        data = ""
        data += self.key_start('input')
        data += self.key_start('twitter', nesting_level=1)
        data += self.item('consumer_key', self.config['CONSUMER_KEY'], nesting_level=2)
        data += self.item('consumer_secret', self.config['CONSUMER_SECRET'], nesting_level=2)
        data += self.item('oauth_token', self.config['OAUTH_TOKEN'], nesting_level=2)
        data += self.item('oauth_token_secret', self.config['OAUTH_TOKEN_SECRET'], nesting_level=2)
        data += self.item('keywords', keywords, nesting_level=2)
        data += self.item('languages', lang, nesting_level=2)
        data += self.item('full_tweet', 'true', nesting_level=2, no_quotes=True)
        data += self.item('ignore_retweets', 'true', nesting_level=2, no_quotes=True)
        data += self.item('tags', [es_index_name], nesting_level=2)
        data += self.key_end(nesting_level=1)
        data += self.key_end(nesting_level=0)
        return data

    def key_start(self, key, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{} {}\n".format(indent, key, '{')

    def key_end(self, nesting_level=0):
        indent = '  '*nesting_level
        return "{}{}\n".format(indent, '}')

    def item(self, key, val, nesting_level=0, no_quotes=False):
        indent = '  '*nesting_level
        if isinstance(val, str) and not no_quotes:
            return '{}{} => "{}"\n'.format(indent, key, val)
        else:
            return '{}{} => {}\n'.format(indent, key, val)

    def parse_twitter_input(self, f_name):
        """Parser for twitter input files"""
        res = {}
        fields_to_parse = ['keywords', 'languages', 'tags']
        f = open(f_name, 'r')
        for l in f.readlines():
            if not '=>' in l:
                continue
            key, val = l.split('=>')
            key = key.strip()
            val = val.strip()

            if key in fields_to_parse:
                # parse string to list
                if key == 'tags':
                    res['es_index_name'] = ast.literal_eval(val)[0]
                else:
                    res[key] = ast.literal_eval(val)

        f.close()
        return res

    def generate_output_file(self, outputs=['redis']):
        data = ""
        # start output
        data += self.key_start('output')

        # redis output
        data += self.key_start('if "redis" in [type]', nesting_level=1)
        data += self.key_start('redis', nesting_level=2)
        data += self.item('id', 'main-output-plugin', nesting_level=3)
        data += self.item('host', self.config['REDIS_HOST'], nesting_level=3)
        data += self.item('port', self.config['REDIS_PORT'], nesting_level=3, no_quotes=True)
        data += self.item('db', '0', nesting_level=3, no_quotes=True)
        data += self.item('data_type', 'list', nesting_level=3)
        data += self.item('codec', 'json', nesting_level=3)
        data += self.item('key', '{}:{}'.format(self.config['REDIS_NAMESPACE'], self.config['REDIS_LOGSTASH_QUEUE_KEY']), nesting_level=3)
        data += self.key_end(nesting_level=2)
        data += self.key_end(nesting_level=1)

	# s3 output
        if not (self.config['AWS_ACCESS_KEY_ID'] == '' or self.config['AWS_SECRET_ACCESS_KEY'] == '') and 'S3_BUCKET' in self.config:
            data += self.key_start('else', nesting_level=1)
            data += self.key_start('s3', nesting_level=2)
            data += self.item('access_key_id', self.config['AWS_ACCESS_KEY_ID'], nesting_level=3)
            data += self.item('secret_access_key', self.config['AWS_SECRET_ACCESS_KEY'], nesting_level=3)
            data += self.item('region', self.config['AWS_REGION'], nesting_level=3)
            data += self.item('bucket', self.config['S3_BUCKET'], nesting_level=3)
            data += self.item('prefix', 'tweets/%{project}/', nesting_level=3)
            data += self.item('time_file', '1440', nesting_level=3, no_quotes=True)
            data += self.item('codec', 'json_lines', nesting_level=3)
            data += self.key_end(nesting_level=2)
            data += self.key_end(nesting_level=1)

        # std output
        # data += self.key_start('stdout', nesting_level=1)
        # data += self.item('codec', 'line { format => "Collected tweet for project %{project}" }', nesting_level=2, no_quotes=True)
        # data += self.key_end(nesting_level=1)

	# end output
        data += self.key_end(nesting_level=0)
        return data
