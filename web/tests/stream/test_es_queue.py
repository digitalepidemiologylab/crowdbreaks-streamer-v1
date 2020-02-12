import pytest
import sys
import pdb
from datetime import datetime, timedelta
import json
import sys;sys.path.append('../../../web/')

class TestESQueue:
    def test_pop_all(self, es_queue, tweet):
        es_queue.push(json.dumps(tweet).encode(), 'test')
        key = es_queue.queue_key('test')
        assert es_queue.num_elements_in_queue(key) == 1
        resp = es_queue.pop_all(key)
        assert es_queue.num_elements_in_queue(key) == 0
        assert len(resp) == 1
        assert isinstance(resp, list)


if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    # @pytest.mark.focus
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main(['-s'])
