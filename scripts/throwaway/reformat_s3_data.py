import sys, os
sys.path.append('..')
sys.path.append('../web/')
from s3_handler import S3Handler
from app.settings import Config
import re
import uuid


def main():
    s3_handler = S3Handler()
    rgx = re.compile(r'(20[0-9][0-9]-[0-9][0-9]-[0-9][0-9])T(\d\d.\d\d)')
    project = 'flu_tracking'
    prefix = 'tweets/old/{}/ls'.format(project)
    c = 0
    for item in s3_handler.iter_items(project, prefix=prefix):
        key = item['Key']
        matches = rgx.search(key)
        if matches is None:
            continue
        date = matches.group(1)
        time = matches.group(2)
        timestamp = ''.join(date.split('-') + time.split('.') + ['00'])
        new_key = 'tweets/old/{}/{}/tweets-{}-{}.jsonl'.format(project, date , timestamp, str(uuid.uuid4()))
        s3_handler.rename(key, new_key)
        if c % 10 == 0:
            print('Copying items... (total: {})'.format(c))
        c += 1

if __name__ == "__main__":
    main()
