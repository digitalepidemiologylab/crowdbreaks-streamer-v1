import sys, os
sys.path.append('..')
sys.path.append('../web/')
from app.stream.s3_handler import S3Handler
import re
import uuid
import json
import base64
import time
from collections import defaultdict


def main():
    s3_handler = S3Handler()
    project = 'mixed_vaccine_and_crispr'
    prefix = 'tweets/old/{}/ls'.format(project)
    c = 0
    for item in s3_handler.iter_items(prefix=prefix):
        key = item['Key']
        # s3_file = json.loads(s3_handler.read(key))
        tweets_crispr = defaultdict(list)
        tweets_vaccine = defaultdict(list)
        tweets_flu = defaultdict(list)
        for line in s3_handler.read_line(key):
            tweet = json.loads(line)
            tweet_decoded = base64.b64decode(tweet['body'])
            tweet_decoded = json.loads(tweet_decoded)['args'][0]
            project = tweet_decoded['project']
            date = tweet_decoded['created_at']
            ts = time.strftime('%Y-%m-%d', time.strptime(tweet_decoded['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            if project == 'project_vaccine_sentiment':
                tweets_vaccine[ts].append(tweet_decoded)
            elif project == 'project_crispr':
                tweets_crispr[ts].append(tweet_decoded)
            elif project == 'project_flu_tracking':
                tweets_flu[ts].append(tweet_decoded)
            else:
                raise Exception('Unexpected project {}'.format(project))

        projects = {'project_vaccine_sentiment': tweets_vaccine, 'project_crispr': tweets_crispr, 'project_flu_tracking': tweets_flu}
        for project in projects.keys():
            data_dict = projects[project]
            for date in data_dict.keys():
                data = b'\n'.join([json.dumps(tweet).encode() for tweet in data_dict[date]])
                timestamp = ''.join(date.split('-') + ['000000'])
                if project == 'project_flu_tracking':
                    s3_key = 'tweets/old/{}/{}/tweets-{}-{}.jsonl'.format('flu_tracking', date, timestamp, str(uuid.uuid4()))
                else:
                    s3_key = 'tweets/{}/{}/tweets-{}-{}.jsonl'.format(project, date, timestamp, str(uuid.uuid4()))

                print('Writing file {}...'.format(s3_key))
                s3_handler.write(data, s3_key)
        s3_handler.delete(key)

if __name__ == "__main__":
    main()
