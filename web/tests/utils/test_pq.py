import pytest
import sys
import pdb


class TestPriorityQueue:
    def test_add(self, pq):
        test_val = '123456'
        pq.add(test_val)
        val = pq.pop()
        pq.add(test_val)
        assert val == test_val

    def test_max_length(self, pq):
        test_vals = range(pq.MAX_QUEUE_LENGTH + 1)
        for t in test_vals:
            pq.add(t)
        assert len(pq) == pq.MAX_QUEUE_LENGTH

    def test_priority_order(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.add('c', priority=2)
        pq.add('d', priority=3)
        assert pq.pop() == 'd'

    def test_priority_order_remove(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.add('c', priority=2)
        pq.add('d', priority=3)
        val1 = pq.pop(remove=True)
        val2 = pq.pop()
        assert val1 == 'd'
        assert val2 == 'c'

    def test_score(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        assert pq.get_score('b') == 1
        assert len(pq) == 2

    def test_remove_lowest(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.remove_lowest_priority()
        assert len(pq) == 1
        assert pq.pop() == 'b'

    def test_update_priority(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.increment_priority('b')
        assert pq.get_score('b') == 2
        pq.increment_priority('b', incr=10) # increment by 10
        assert pq.get_score('b') == 12

    def test_key_removal(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.remove('b')
        assert len(pq) == 1
        assert pq.pop() == 'a'

    def test_ordering_in_same_priority_values(self, pq):
        pq.add('a', priority=0)
        pq.add('b', priority=0)
        pq.add('c', priority=0)
        pq.add('d', priority=0)
        assert pq.pop() == 'd'

    @pytest.mark.focus
    def test_multi_pop(self, pq):
        pq.add('e', priority=3)
        pq.add('a', priority=0)
        pq.add('b', priority=1)
        pq.add('c', priority=2)
        pq.add('d', priority=0)
        items = pq.multi_pop(3)
        assert set(items) == set(['c', 'e', 'b'])

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
