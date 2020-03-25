import pytest
import sys; sys.path.append('../..')
import time

class TestDataDumpIds:
    @pytest.mark.focus
    def test_add(self, data_dump_ids):
        data_dump_ids.add('123')
        assert data_dump_ids.num_members() == 1

    @pytest.mark.focus
    def test_pop_all(self, data_dump_ids):
        data_dump_ids.add('1')
        data_dump_ids.add('2')
        data_dump_ids.add('3')
        data_dump_ids.add('2')
        assert data_dump_ids.num_members() == 3
        data = data_dump_ids.pop_all()
        assert set(data) == set(['1', '2', '3'])

    @pytest.mark.focus
    def test_file_exists(self, data_dump_ids):
        data_dump_ids.add('1')
        data_dump_ids.add('2')
        data_dump_ids.add('3')
        data_dump_ids.add('2')
        data_dump_ids.sync()


if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
