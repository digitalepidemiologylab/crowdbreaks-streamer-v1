import pytest
import sys; sys.path.append('../..')
from app.utils.process_tweet import ProcessTweet

class TestProcessTweet:
    def test_pt(self, tweet):
        pt = ProcessTweet(tweet=tweet, project='project_test')
        assert pt.is_matching_project_locales()
        pt = ProcessTweet(tweet=tweet, project='project_test', project_locales=['en'])
        assert pt.is_matching_project_locales()
        pt = ProcessTweet(tweet=tweet, project='project_test', project_locales=['de'])
        assert not pt.is_matching_project_locales()

    def test_should_be_annotated(self, tweet):
        pt = ProcessTweet(tweet=tweet, project='project_test', project_locales=['en'])
        processed_tweet = pt.process_and_predict()
        assert pt.should_be_annotated()
        pt = ProcessTweet(tweet=tweet, project='project_test', project_locales=['de'])
        processed_tweet = pt.process_and_predict()
        assert not pt.should_be_annotated()

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    # @pytest.mark.focus
    # pytest.main(['-s', '-m', 'focus'])
    pytest.main()
