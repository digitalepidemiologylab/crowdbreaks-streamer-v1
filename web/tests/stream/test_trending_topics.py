import pytest
import sys; sys.path.append('../..')
import time
import json
from app.utils.process_tweet import ProcessTweet

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
        assert len(trending_topics.pq) == 0
        trending_topics.process(tweet)
        assert len(trending_topics.pq) == 1

    @pytest.mark.focus
    def test_forget(self, trending_topics, tweet):
        for _ in range(10):
            trending_topics.process(tweet)
        assert len(trending_topics.pq) == 1
        trending_topics.forget_topics()
        key = trending_topics.pq.pop()
        score = trending_topics.pq.get_score(key)
        assert score == 9

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
