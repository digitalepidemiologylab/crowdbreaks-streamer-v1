from app.stream.stream_config_reader import StreamConfigReader
from app.utils.reverse_tweet_matcher import ReverseTweetMatcher


if __name__ == "__main__":
    import json
    import os
    with open(os.path.join('app', 'config', 'example_data', 'tweet.json'), 'r') as f:
        tweet = json.load(f)
    rtm = ReverseTweetMatcher(tweet=tweet)
    print(rtm.get_candidates())

