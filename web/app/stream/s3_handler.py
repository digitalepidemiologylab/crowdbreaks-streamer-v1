import redis
from app.settings import Config
import boto3
import logging
from helpers import report_error
import botocore.exceptions

logger = logging.getLogger(__name__)

class S3Handler():
    """Handles queuing in Redis and pushing tweets to S3"""

    def __init__(self, bucket=None):
        self.config = Config()
        if bucket is None:
            self.bucket = self.config.S3_BUCKET
        elif bucket == 'public':
            self.bucket = self.config.S3_BUCKET_PUBLIC
        elif bucket == 'sagemaker':
            self.bucket = self.config.S3_BUCKET_SAGEMAKER
        else:
            raise ValueError(f'Unknown bucket {bucket}')

    def upload_to_s3(self, content, key):
        try:
            self._s3_client.put_object(Body=content, Bucket=self.bucket, Key=key)
        except Exception as e:
            report_error(logger, exception=True)
            return False
        else:
            return True

    def upload_file(self, local_path, key, make_public=False):
        extra_args = None
        if make_public:
            extra_args = {'ACL': 'public-read'}
        try:
            self._s3_client.upload_file(local_path, self.bucket, key, ExtraArgs=extra_args)
        except Exception as e:
            report_error(logger, exception=True)
            return False
        else:
            return True

    def download_file(self, local_path, key):
        try:
            self._s3_client.download_file(self.bucket, key, local_path)
        except Exception as e:
            report_error(logger, exception=True)
            return False
        else:
            return True

    def file_exists(self, key):
        try:
            self._s3_client.head_object(Bucket=self.bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                report_error(logger, exception=True)
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
