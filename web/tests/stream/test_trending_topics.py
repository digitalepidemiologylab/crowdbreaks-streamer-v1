import pytest
import sys; sys.path.append('../..')
import time
import json

class TestTrendingTopics:
    def test_tokenize_text(self, trending_topics):
        text = 'Donald Trump should be properly tokenized.'
        tokens = trending_topics.tokenize(text)
        assert len(tokens) == 1
        assert tokens[0] == 'Donald Trump'

    def test_ignores_blacklisted(self, trending_topics):
        text = 'The word test is part of the project keywords and therefore blacklisted.'
        tokens = trending_topics.tokenize(text)
        assert 'test' not in tokens

    def test_process(self, trending_topics, tweet):
        assert len(trending_topics.pq_counts) == 0
        trending_topics.process(tweet)
        assert len(trending_topics.pq_counts) == 1

    def test_velocity(self, trending_topics, tweet):
        assert len(trending_topics.pq_velocity) == 0
        for _ in range(10):
            trending_topics.process(tweet)
        assert len(trending_topics.pq_counts) == 1
        assert len(trending_topics.pq_velocity) == 0
        # first time we compute velocity we just copy over the current into old
        trending_topics.compute_velocity()
        assert len(trending_topics.pq_counts_old) == 1
        assert len(trending_topics.pq_counts) == 0
        assert len(trending_topics.pq_velocity) == 0
        for _ in range(5):
            trending_topics.process(tweet)
        assert len(trending_topics.pq_velocity) == 0
        assert len(trending_topics.pq_counts) == 1
        # second time we can compute a velocity
        trending_topics.compute_velocity()
        assert len(trending_topics.pq_velocity) == 1
        assert trending_topics.pq_velocity.pop() == 'text'
        # make sure topic is below
        trending_topics.pq_velocity.get_score('text') < 0

    def test_velocity_2(self, trending_topics, tweet):
        # add tweet 10 times
        for _ in range(10):
            trending_topics.process(tweet)
        # compute over to old state
        trending_topics.compute_velocity()
        # add another 15 times
        for _ in range(15):
            trending_topics.process(tweet)
        # compute velocity
        trending_topics.compute_velocity()
        assert len(trending_topics.pq_velocity) == 1
        term = trending_topics.pq_velocity.pop()
        assert term == 'text'
        assert trending_topics.pq_velocity.get_score(term) > 0

    def test_retweet_tweet_count(self, trending_topics, tweet, retweet):
        trending_topics.process(tweet)
        trending_topics.process(retweet)
        assert len(trending_topics.pq_counts) == 2
        assert trending_topics.pq_counts.get_score('text') == 1
        assert trending_topics.pq_counts.get_score('tweet') == 0.2
        assert len(trending_topics.pq_counts_retweets) == 1
        assert len(trending_topics.pq_counts_tweets) == 1

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
