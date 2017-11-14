# Default configs
DEBUG=True

# Logging
WORKER_LOGFILE='worker.log'

# Redis
REDIS_HOST='localhost'
REDIS_PORT=6379
REDIS_DB=0
REDIS_NAMESPACE='crowdbreaks'
REDIS_LOGSTASH_QUEUE_KEY='logstash'
REDIS_SUBMIT_QUEUE_KEY='es_submit'
REDIS_SENTIMENT_QUEUE_KEY='sentiment'

# Elasticsearch
ELASTICSEARCH_HOST='localhost'
ELASTICSEARCH_PORT='9200'

# Num processes
NUM_PROCESSES_PREPROCESSING=2
NUM_SUBMIT_PREPROCESSING=2
NUM_SENTIMENT_PREPROCESSING=2



