import pytest
import sys; sys.path.append('../..')
import time
import json

class TestTrendingTopics:
    @pytest.mark.focus
    def test_tokenize_text(self, trending_topics):
        text = 'South Korea'
        tokens = trending_topics.tokenize(text)
        assert len(tokens) == 1

    @pytest.mark.focus
    def test_ignores_blacklisted(self, trending_topics):
        text = 'The word test is part of the project keywords and therefore blacklisted.'
        tokens = trending_topics.tokenize(text)
        assert 'test' not in tokens

    def test_process(self, trending_topics, tweet):
        assert len(trending_topics.pq_counts_weighted) == 0
        trending_topics.process(tweet)
        assert len(trending_topics.pq_counts_weighted) == 1

    def test_retweet_tweet_count(self, trending_topics, tweet, retweet):
        trending_topics.process(tweet)
        trending_topics.process(retweet)
        assert len(trending_topics.pq_counts_weighted) == 2
        assert trending_topics.pq_counts_weighted.get_score('text') == 1
        assert trending_topics.pq_counts_weighted.get_score('tweet') < 1
        assert len(trending_topics.pq_counts_retweets) == 1
        assert len(trending_topics.pq_counts_tweets) == 1

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
