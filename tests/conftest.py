import pytest
import sys
sys.path.append("..")
from worker.priority_queue import PriorityQueue, TweetIdQueue, RedisSet


# vars global to test env
def pytest_namespace():
    return {'max_queue_length': 3,
            'priority_threshold': 3}


# session fixtures
@pytest.fixture(scope='session')
def pq():
    pq = PriorityQueue('test_project', namespace='test', max_queue_length=pytest.max_queue_length)
    yield pq
    pq.self_remove()

@pytest.fixture(scope='session')
def rs():
    rs = RedisSet('test_project', namespace='test')
    yield rs
    rs.self_remove_all()

@pytest.fixture(scope='session')
def tid_q():
    tid_q = TweetIdQueue('test_project', namespace='test', priority_threshold=pytest.priority_threshold)
    yield tid_q
    tid_q.flush()
