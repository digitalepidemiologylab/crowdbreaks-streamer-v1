import pytest
import sys; sys.path.append('../..')
import time

class TestTrendingTweets:
    def test_does_not_add_tweet(self, tweet, tt):
        tt.process(tweet)
        assert len(tt.pq) == 0

    def test_does_add_retweet(self, retweet, tt):
        tt.process(retweet)
        retweeted_id = retweet['retweeted_status']['id_str']
        assert len(tt.pq) == 1
        assert tt._r.exists(tt.expiry_key(retweeted_id))

    @pytest.mark.focus
    def test_expiry(self, retweet, tt):
        assert len(tt.pq) == 0
        retweeted_id = retweet['retweeted_status']['id_str']
        tt.process(retweet)
        tt.cleanup()
        assert len(tt.pq) == 1
        time.sleep(.01)  # wait 10ms
        assert not tt._r.exists(tt.expiry_key(retweeted_id))
        # pq is wiped after cleanup
        assert len(tt.pq) == 1
        tt.cleanup()
        assert len(tt.pq) == 0

    def test_max_queue_length(self, retweet, tt):
        assert len(tt.pq) == 0
        for retweet_id in range(6):
            retweet['retweeted_status']['id_str'] = str(retweet_id)
            tt.process(retweet)
        # max queue length is 5!
        assert len(tt.pq) == 5

    def test_increment(self, retweet, tt):
        assert len(tt.pq) == 0
        retweeted_id = retweet['retweeted_status']['id_str']
        for retweet_id in range(6):
            # same retweet is processed
            tt.process(retweet)
        # max queue length is 5!
        assert len(tt.pq) == 1
        assert tt.pq.get_score(retweeted_id) == 6

    def test_pop(self, retweet, tt):
        retweet['retweeted_status']['id_str'] = '0'
        # add 0 twice (should be highest priority)
        for _ in range(2):
            tt.process(retweet)
        retweet['retweeted_status']['id_str'] = '1'
        tt.process(retweet)
        assert tt.pq.pop() == '0'
        # add '1' 3 more time, making it highest priority
        for _ in range(3):
            tt.process(retweet)
        assert tt.pq.pop() == '1'


if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    # pytest.main(['-s', '-m', 'focus'])
    pytest.main()
