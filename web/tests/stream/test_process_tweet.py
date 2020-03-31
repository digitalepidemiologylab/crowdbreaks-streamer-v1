import pytest
import sys; sys.path.append('../..')
from app.utils.process_tweet import ProcessTweet

class TestProcessTweet:
    def test_pt(self, tweet):
        pt = ProcessTweet(tweet)
        assert pt.is_matching_project_locales()
        pt = ProcessTweet(tweet, project_locales=['en'])
        assert pt.is_matching_project_locales()
        pt = ProcessTweet(tweet, project_locales=['de'])
        assert not pt.is_matching_project_locales()

    def test_should_be_annotated(self, tweet):
        pt = ProcessTweet(tweet, project_locales=['en'])
        pt.process()
        assert pt.should_be_annotated()
        pt = ProcessTweet(tweet, project_locales=['de'])
        assert not pt.should_be_annotated()

    def test_has_place(self, tweet, tweet_with_place):
        pt = ProcessTweet(tweet)
        assert not pt.has_place
        pt = ProcessTweet(tweet_with_place)
        assert pt.has_place

    def test_has_coords(self, tweet, tweet_with_coordinates):
        pt = ProcessTweet(tweet)
        assert not pt.has_coordinates
        pt = ProcessTweet(tweet_with_coordinates)
        assert pt.has_coordinates

    @pytest.mark.focus
    def test_compute_average_locatio(self, tweet_with_place):
        pt = ProcessTweet(tweet_with_place)
        pt.compute_average_location()
        assert pt.processed_tweet['place'] == {'average_location': [-105.14544, 40.192138], 'location_radius': 0.0}

if __name__ == "__main__":
    # if running outside of docker, make sure redis is running on localhost
    # @pytest.mark.focus
    pytest.main(['-s', '-m', 'focus'])
    # pytest.main()
