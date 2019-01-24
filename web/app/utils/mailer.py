import logging
from app.settings import Config
from datetime import datetime, timedelta
from app.stream.stream_config_reader import StreamConfigReader
from app.stream.redis_s3_queue import RedisS3Queue
import os
import re
import subprocess
import mandrill
from helpers import get_tz_difference
import pytz

class Mailer():
    """Handles Emailing"""
    def __init__(self):
        self.config = Config()
        self.client = mandrill.Mandrill(self.config.MANDRILL_API_KEY)
        self.logger = logging.getLogger(__name__)

    def send(self, msg):
        return self.client.messages.send(msg)


class StreamStatusMailer(Mailer):
    def __init__(self, status_type='daily'):
        super().__init__()
        self.status_type = status_type
        self.from_addr = self.config.EMAIL_FROM_ADDR
        if self.status_type == 'daily':
            self.to_addr = self.config.EMAIL_STREAM_STATUS_DAILY
        elif self.status_type == 'weekly':
            self.to_addr = self.config.EMAIL_STREAM_STATUS_WEEKLY
        else:
            raise Exception('Status type {} is not recognized'.format(self.status_type))

    def get_body(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.status_type == 'daily':
            projects_stats, total_count = self._get_projects_stats(num_days=1, hourly=True)
            total_text = 'Total today'
        else:
            projects_stats, total_count = self._get_projects_stats()
            total_text = 'Total this week'
        html_text = """\
            <html>
              <head></head>
                <body>
                    <h2>Crowdbreaks stream status</h2>
                    Date: {date}<br>
                    {total_text}: {total_count:,}<br>
                    {projects_stats}
                    <h2>Error log (past 7 days)</h2>
                    {errors}
                </body>
            </html>
        """.format(date=today, total_text=total_text, total_count=total_count, projects_stats=projects_stats, errors=self._get_error_log(5), subtype='html')
        return html_text

    def _get_projects_stats(self, num_days=7, hourly=False):
        stream_config_reader = StreamConfigReader()
        redis_s3_queue = RedisS3Queue()
        end_day = datetime.utcnow()
        start_day = end_day - timedelta(days=num_days)
        stats = ''
        dates = list(redis_s3_queue.daterange(start_day, end_day, hourly=hourly))
        now_utc = pytz.utc.localize(end_day)
        timezone_hour_delta = get_tz_difference()
        total = 0
        for stream in stream_config_reader.read():
            project = stream['es_index_name']
            project_slug = stream['slug']
            stats += "<h3>{}</h3>".format(project)
            total_by_project = 0
            for d in dates:
                if hourly:
                    d, h = d.split(':')
                    count = redis_s3_queue.get_counts(project_slug, d, h)
                    corrected_hour = (datetime.strptime(h, '%H') - timezone_hour_delta).strftime('%H')
                    stats += '{0} ({1}:00 - {1}:59): {2:,}<br>'.format(d, corrected_hour, count)
                else:
                    count = redis_s3_queue.get_counts(project_slug, d)
                    stats += '{}: {:,}<br>'.format(d, count)
                total += count
                total_by_project += count
            stats += 'Total: {:,}<br>'.format(total_by_project)
        return stats, total

    def _get_error_log(self, n=1, num_days=7, max_length=30):
        error_log = os.path.join(self.config.PROJECT_ROOT, 'logs', 'error.log')
        output = ''
        if os.path.isfile(error_log):
            # Use tail in order to prevent going through the full error log
            proc = subprocess.Popen(['tail', '-n', str(max_length), error_log], stdout=subprocess.PIPE)
            lines = proc.stdout.readlines()
            output += '<pre>'
            ignore_beyond = datetime.now() - timedelta(days=num_days)
            for line in lines:
                line = line.decode()
                # find timestamp in current error log line
                match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                if not match is None:
                    try:
                        date = datetime.strptime(match[0], '%Y-%m-%d %H:%M:%S')
                    except:
                        # discard line if date cannot be parsed
                        continue
                    if date > ignore_beyond:
                        output += line
            output += '</pre>'
        return output

    def send_status(self, body):
        msg = {
                'html': body,
                'from_email': self.from_addr,
                'to': [{
                    'email': self.to_addr
                    }],
                'subject': 'Crowdbreaks {} stream update'.format(self.status_type),
                }
        return self.send(msg)
