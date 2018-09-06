import pytest

class TestRedisSet:
    def test_add(self, rs):
        rs.add('a', 'user')
        assert rs.is_member('a', 'user')
        rs.self_remove_all()

    def test_remove(self, rs):
        assert rs.num_members('a') == 0
        rs.add('a', 'user_a')
        rs.add('a', 'user_b')
        assert rs.num_members('a') == 2
        rs.remove('a')
        rs.remove('a') # Remove non-existant key shouldn't throw any error
        assert not rs.is_member('a', 'user_a')
        assert rs.num_members('a') == 0
        rs.self_remove_all()

    def test_dont_add_same_user_twice(self, rs):
        assert rs.num_members('b') == 0
        rs.add('b', 'user_a')
        rs.add('b', 'user_a')
        assert rs.num_members('b') == 1
        assert rs.is_member('b', 'user_a')
        rs.self_remove_all()


if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main()
