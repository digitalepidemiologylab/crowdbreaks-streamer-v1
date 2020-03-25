import pytest
import sys; sys.path.append('../..')
import time

class TestDataDumpIds:
    @pytest.mark.focus
    def test_add(self, data_dump_ids):
        data_dump_ids.add('123')
        assert len(data_dump_ids) == 1

    @pytest.mark.focus
    def test_pop_all(self, data_dump_ids):
        data_dump_ids.add('1')
        data_dump_ids.add('2')
        data_dump_ids.add('3')
        assert len(data_dump_ids) == 3
        data = data_dump_ids.pop_all()
        assert set(data) == set(['1', '2', '3'])

    @pytest.mark.focus
    def test_pop_all_iter(self, data_dump_ids):
        for i in range(100):
            data_dump_ids.add(str(i))
        num_chunks = 0
        for chunk in data_dump_ids.pop_all_iter(chunk_size=10):
            num_chunks += 1
            assert len(chunk) == 10
        assert num_chunks == 10

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
