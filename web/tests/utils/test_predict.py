import pytest
import sys; sys.path.append('../..')
from app.utils.predict import Predict

class TestPredict:
    @pytest.mark.focus
    def test_preprocess(self, predictor):
        text_obj = ['This is an example Text <@user>']
        tokenized_text = predictor.preprocess_text(text_obj)
        assert tokenized_text[0] == 'this be a example text user'

    @pytest.mark.focus
    def test_predict(self):
        predictor = Predict('crowdbreaks-331b8421d3', 'fasttext')
        predict_obj = ['This is an example Text <@user>']
        predictions = predictor.predict(predict_obj)
        assert len(predictions) == 1
        assert set(predictions[0].keys()) == set(['labels', 'probabilities', 'label_vals'])

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    import os; os.environ["REDIS_HOST"] = "localhost"
    pytest.main(['-s', '-m', 'focus'])
