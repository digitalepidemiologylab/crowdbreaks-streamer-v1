import pytest
import sys;sys.path.append('../../../web/')


class TestRedis:
    def test_get_cached(self, r):
        key = 'cache_key'
        some_data = {'hello': 'world', 'a_val': 1.2313221, 'empty_dict': {}}
        r.set_cached(key, some_data)
        retrieved = r.get_cached(key)
        assert retrieved == some_data

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
