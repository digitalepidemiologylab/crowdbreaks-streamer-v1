import pytest

class TestPriorityQueue:
    def test_increase_priority_on_update(self, tid_q):
        tid_q.pq.add('123456')
        assert tid_q.pq.get_score('123456') == 0   # default priority
        tid_q.update('123456', 'some_user')
        assert tid_q.pq.get_score('123456') == 1   # classified once
        tid_q.flush()

    def test_tweet_store(self, tweet_store, tweet):
        no_old_tweet_present = tweet_store.get(tweet['id'])
        assert no_old_tweet_present is None
        # add and then get again
        tweet_store.add(tweet)
        retweet_from_tweet_store = tweet_store.get(tweet['id'])
        assert retweet_from_tweet_store == tweet

    def test_tweet_id_queue_with_full_tweets(self, tid_q, tweet):
        tid_q.add_tweet(tweet)
        r_tweet = tid_q.get_tweet()
        assert r_tweet == tweet
        tid_q.flush()

    def test_increase_over_threshold(self, tid_q):
        tweet_id = '123456'
        tid_q.pq.add(tweet_id)
        for i in range(tid_q.priority_threshold - 1):
            tid_q.update(tweet_id, 'user'+str(i))
        assert tid_q.pq.get_score(tweet_id) == tid_q.priority_threshold - 1
        assert len(tid_q.pq) == 1
        assert tid_q.rset.is_member(tweet_id, 'user0')

        # reaching threshold
        tid_q.update(tweet_id, 'final_user')
        assert len(tid_q.pq) == 0
        assert not tid_q.rset.is_member(tweet_id, 'user0')
        assert not tid_q.rset.is_member(tweet_id, 'final_user')
        assert tid_q.rset.num_members(tweet_id) == 0
        tid_q.flush()

    def test_get_highest_priority_item_full_tweet(self, tid_q, tweet, retweet):
        tid_q.add_tweet(tweet)
        tweet_id = tweet['id_str']
        tid_q.update(tweet_id, 'the_dude')
        assert tid_q.get(user_id='the_dude') is None  # has already classified this tweet
        assert tid_q.get(user_id='another_dude') == tweet_id
        tid_q.update(tweet_id, 'another_dude')
        assert tid_q.get(user_id='another_dude') is None  # has already classified this tweet

        # new tweet is added to queue
        tid_q.add_tweet(retweet)
        new_tweet_id = retweet['id_str']
        assert new_tweet_id != tweet_id
        assert tid_q.get(user_id='another_dude') == new_tweet_id
        assert tid_q.get(user_id='the_dude') == new_tweet_id
        tid_q.flush()

    def test_get_highest_priority_item(self, tid_q):
        tweet_id = '123456'
        tid_q.pq.add(tweet_id)
        tid_q.update(tweet_id, 'the_dude')
        assert tid_q.get(user_id='the_dude') is None  # has already classified this tweet
        assert tid_q.get(user_id='another_dude') == tweet_id
        tid_q.update(tweet_id, 'another_dude')
        assert tid_q.get(user_id='another_dude') is None  # has already classified this tweet

        new_tweet_id = '654321' # new tweet is added to queue
        tid_q.pq.add(new_tweet_id)
        assert tid_q.get(user_id='another_dude') == new_tweet_id
        assert tid_q.get(user_id='the_dude') == new_tweet_id
        tid_q.flush()

    def test_update_in_empty_queue(self, tid_q):
        # don't allow update on unknown key
        tid_q.pq.add('123')
        tid_q.update('123456', 'user_a')
        assert len(tid_q.pq) == 1
        tid_q.flush()

    def test_get_without_user(self, tid_q):
        tid_q.pq.add('321', priority=1)
        tid_q.pq.add('123', priority=0)
        tid_q.update('321', 'user_a')
        assert tid_q.get(user_id='user_a') == '123'
        assert tid_q.get() == '321'  # empty user_id gives highest priority element

    def test_removal_tweet(self, tid_q):
        tid_q.pq.add('321', priority=1)
        tid_q.pq.add('123', priority=0)
        tid_q.update('321', 'user_a')
        assert len(tid_q.pq) == 2
        tid_q.remove('321')
        tid_q.remove('21')
        assert len(tid_q.pq) == 1
        assert tid_q.rset.num_members('321') == 0
        assert tid_q.rset.num_members('123') == 0



if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    # @pytest.mark.focus
    # pytest.main(['-s', '-m', 'focus'])
    pytest.main()
