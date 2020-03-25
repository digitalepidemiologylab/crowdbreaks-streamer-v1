from app.utils.redis import Redis
from app.stream.s3_handler import S3Handler
import logging
from app.settings import Config
import time
import os
import uuid
import shutil
from helpers import report_error

logger = logging.getLogger(__name__)

class DataDumpIds(Redis):
    def __init__(self, project, namespace='cb', key_namespace='data-dump-ids', **args):
        super().__init__(self)
        self.project = project
        self.namespace = namespace
        self.key_namespace = key_namespace
        self.config = Config()
        self.tmp_path = os.path.join(self.config.APP_DIR, 'tmp')
        self.data_dump_f_name = f'data_dump_ids_{self.project}'
        self.local_file = os.path.join(self.tmp_path, self.data_dump_f_name)
        self.local_file_tmp = os.path.join(self.tmp_path, self.data_dump_f_name + '.tmp')
        self.data_dump_key = f'data_dump/{self.project}/{self.data_dump_f_name}.txt'
        self.s3_handler = S3Handler(bucket='public')

    @property
    def key(self):
        return "{}:{}:{}".format(self.namespace, self.key_namespace, self.project)

    def add(self, value):
        self._r.sadd(self.key, value)

    def self_remove(self):
        self._r.delete(self.key)

    def num_members(self):
        return self._r.scard(self.key)

    def pop_all(self):
        return [d.decode() for d in self._r.spop(self.key, count=self.num_members())]

    def download_existing_data_dump(self):
        logger.info(f'Downloading existing data dump with key {self.data_dump_key} to {self.local_file}...')
        success = self.s3_handler.download_file(self.local_file, self.data_dump_key)
        return success

    def sync(self):
        new_data = self.pop_all()
        num_new_data = len(new_data)
        if num_new_data == 0:
            logger.info(f'No new data was collected. Aborting.')
            return
        logger.info(f'Writing {len(new_data):,} to file {self.data_dump_key}...')
        with open(self.local_file_tmp, 'w') as f:
            f.write('\n'.join(new_data))
        logger.info(f'Collected {num_new_data:,} ids')
        if self.s3_handler.file_exists(self.data_dump_key):
            success = self.download_existing_data_dump()
            if not success:
                logger.error(f'Something went wrong when trying to download the existing data. Aborting.')
                return
            # concatenating new data
            with open(self.local_file, 'a') as f:
                f.write('\n')
                shutil.copyfileobj(open(self.local_file_tmp, 'r'), f)
        else:
            # There is no existing data, simply rename file
            os.rename(self.local_file_tmp, self.local_file)
        # reuploading file
        logger.info(f'Uploading file to S3 under key {self.data_dump_key}')
        success = self.s3_handler.upload_file(self.local_file, self.data_dump_key, make_public=True)
        if not success:
            report_error(logger, msg='Uploading data dump Ids file to S3 unsuccessful.')
        # cleanup
        logger.info('Cleaning up temporary files...')
        for f in [self.local_file, self.local_file_tmp]:
            if os.path.isfile(f):
                os.remove(f)
