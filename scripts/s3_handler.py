from app.settings import Config
import boto3

class S3Handler():
    def __init__(self):
        self.config = Config()
        self.bucket = self.config.S3_BUCKET

    def iter_items(self, prefix=''):
        return self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix)['Contents']

    def rename(self, old_key, new_key):
        copy_source = {'Bucket': self.bucket, 'Key': old_key}
        key = self.bucket + '/' + new_key
        self.s3_client.copy_object(Bucket=self.bucket, CopySource=copy_source, Key=new_key)
        self.delete(old_key)

    def delete(self, key):
        self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def read(self, key):
        return self.s3_client.get_object(Bucket=self.bucket, Key=key)['Body'].read().decode()

    def read_line(self, key):
        return self.s3_client.get_object(Bucket=self.bucket, Key=key)['Body'].read().splitlines()
    
    def write(self, content, key):
        self.s3_client.put_object(Body=content, Bucket=self.bucket, Key=key)

    @property
    def s3_client(self):
        return boto3.client('s3')
