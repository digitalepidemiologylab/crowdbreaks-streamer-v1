import pytest

class TestTweetStore:
    def test_add(self, tweet_store, tweet):
        tweet_store.add(tweet['id'], tweet)
        assert len(tweet_store) == 1

    def test_cleanup_stale(self, tweet_store, tweet):
        tweet_store.add(tweet['id'], tweet)
        assert len(tweet_store) == 1
        # tweet was never added to pq, therefore should be removed by cleanup
        tweet_store.cleanup(['test_project'])
        assert len(tweet_store) == 0

    def test_cleanup_non_stale(self, tweet_store, pq, tweet):
        tweet_store.add(tweet['id_str'], tweet)
        pq.add(tweet['id_str'])
        assert len(tweet_store) == 1
        assert len(pq) == 1
        # tweet was added to pq, do not remove from tweet store
        tweet_store.cleanup(['test_project'])
        assert len(tweet_store) == 1

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
