import pytest
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from app.utils.priority_queue import PriorityQueue, TweetIdQueue, RedisSet
from app.stream.redis_s3_queue import RedisS3Queue


# vars global to test env
def pytest_namespace():
    return {'max_queue_length': 10,
            'priority_threshold': 5}


# session fixtures
@pytest.fixture(scope='session')
def pq():
    pq = PriorityQueue('test_project', namespace='test', **pytest.__dict__)
    yield pq
    pq.self_remove()

@pytest.fixture(scope='session')
def rs():
    rs = RedisSet('test_project', namespace='test')
    yield rs
    rs.self_remove_all()

@pytest.fixture(scope='session')
def tid_q():
    tid_q = TweetIdQueue('test_project', namespace='test', **pytest.__dict__)
    yield tid_q
    tid_q.flush()

@pytest.fixture(scope='session')
def s3_q():
    redis_s3_queue = RedisS3Queue() 
    yield redis_s3_queue
    redis_s3_queue.clear()
