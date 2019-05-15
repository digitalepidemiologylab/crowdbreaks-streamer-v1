import pytest
import sys
import boto3
import os
import requests
from unittest.mock import patch
sys.path.append('../../')
from app.utils.process_media import ProcessMedia
import urllib

class TestProcessMedia:
    def test_process_iamges(self, tweet_with_images, s3_q):
        pm = ProcessMedia(tweet_with_images)
        a = patch('app.utils.process_media.urllib')
        b = patch('app.stream.s3_handler.S3Handler.upload_file', return_value=True)
        s3_q.clear_all_counts()
        with a, b:
            pm.process()
        assert s3_q.get_counts(pm.project, media_type='photo') == 1
        s3_q.clear_all_counts()

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main()
