import pytest

class TestPredictQueue:
    def test_push(self, predict_queue, tweet):
        d = {'id': str(tweet['id']), 'text': tweet['text']}
        predict_queue.push(d)
        assert len(predict_queue) == 1

    def test_pop_all(self, predict_queue, tweet, retweet):
        d1 = {'id': str(tweet['id']), 'text': tweet['text']}
        d2 = {'id': str(retweet['id']), 'text': retweet['text']}
        predict_queue.multi_push([d1, d2])
        assert len(predict_queue) == 2
        resp = predict_queue.pop_all()
        assert len(resp) == 2
        assert resp[0]['text'] == tweet['text']
        assert resp[1]['text'] == retweet['text']

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
