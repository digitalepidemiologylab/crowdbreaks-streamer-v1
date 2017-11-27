# Default configs
DEBUG=True

# Logging
WORKER_LOGFILE='worker.log'

# Redis
REDIS_NAMESPACE='crowdbreaks'
REDIS_LOGSTASH_QUEUE_KEY='logstash'
REDIS_SUBMIT_QUEUE_KEY='es_submit'
REDIS_EMBEDDING_QUEUE_KEY='embedding'
REDIS_EMBEDDING_RESULT_QUEUE_KEY='embedding_result'

# Elasticsearch (define in instance/config.py)

# Num processes
NUM_PROCESSES_PREPROCESSING=2
NUM_SUBMIT_PREPROCESSING=2
NUM_EMBEDDING_PREPROCESSING=1



