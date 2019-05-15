import redis
from app.settings import Config
import boto3
import logging
from helpers import report_error


class S3Handler():
    """Handles queuing in Redis and pushing tweets to S3"""

    def __init__(self):
        self.config = Config()
        self.bucket = self.config.S3_BUCKET
        self.logger = logging.getLogger(__name__)

    def upload_to_s3(self, content, key):
        try: 
            self._s3_client.put_object(Body=content, Bucket=self.bucket, Key=key)
        except Exception as e:
            report_error(self.logger, e)
            return False
        else:
            return True

    def upload_file(self, local_path, key):
        try: 
            self._s3_client.upload_file(local_path, self.bucket, key)
        except Exception as e:
            report_error(self.logger, e)
            return False
        else:
            return True

    def list_buckets(self):
        return self._s3_client.list_buckets()

    def iter_items(self, prefix=''):
        return self._s3_client.list_objects(Bucket=self.bucket, Prefix=prefix)['Contents']

    def rename(self, old_key, new_key):
        copy_source = {'Bucket': self.bucket, 'Key': old_key}
        key = self.bucket + '/' + new_key
        self._s3_client.copy_object(Bucket=self.bucket, CopySource=copy_source, Key=new_key)
        self.delete(old_key)

    def delete(self, key):
        self._s3_client.delete_object(Bucket=self.bucket, Key=key)

    def read(self, key):
        return self._s3_client.get_object(Bucket=self.bucket, Key=key)['Body'].read().decode()

    def read_line(self, key):
        return self._s3_client.get_object(Bucket=self.bucket, Key=key)['Body'].read().splitlines()
        
    # private methods

    @property
    def _s3_client(self):
        return boto3.client('s3')
