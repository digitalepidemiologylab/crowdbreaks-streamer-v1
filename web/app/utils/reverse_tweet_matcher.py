import re
import logging
import os
from app.stream.stream_config_reader import StreamConfigReader
from collections import defaultdict

class ReverseTweetMatcher(object):
    """Tries to reverse match a tweet object given a set of keyword lists and languages."""

    def __init__(self, tweet=None):
        self.is_retweet = self._is_retweet(tweet)
        self.tweet = self._get_tweet(tweet)
        self.logger = logging.getLogger(__name__)
        self.stream_config_reader = StreamConfigReader()
        self.relevant_text = ''
        self.matching_keywords = {}

    def get_candidates(self):
        relevant_text = self.fetch_all_relevant_text()
        config = self.stream_config_reader.read()
        if len(config) == 0:
            return []
        elif len(config) == 1:
            # only one possibility
            self._find_matching_keywords_for_project(relevant_text, config[0])
            return [config[0]['slug']]
        else:
            # try to match to configs
            return self._match_to_config(relevant_text, config)
    
    def fetch_all_relevant_text(self):
        """Here we pool all relevant text within the tweet to do the matching. From the twitter docs:
        "Specifically, the text attribute of the Tweet, expanded_url and display_url for links and media, text for hashtags, and screen_name for user mentions are checked for matches."
        https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/basic-stream-parameters.html
        """
        text = ''
        if 'extended_tweet' in self.tweet:
            text += self.tweet['extended_tweet']['full_text']
            text += self._fetch_user_mentions(self.tweet['extended_tweet'])
            text += self._fetch_urls(self.tweet['extended_tweet'])
        else:
            text += self.tweet['text']
            text += self._fetch_user_mentions(self.tweet)
            text += self._fetch_urls(self.tweet)

        # pool together with text from quoted tweet
        if 'quoted_status' in self.tweet:
            if 'extended_tweet' in self.tweet['quoted_status']:
                text += self.tweet['quoted_status']['extended_tweet']['full_text']
                text += self._fetch_user_mentions(self.tweet['quoted_status']['extended_tweet'])
                text += self._fetch_urls(self.tweet['quoted_status']['extended_tweet'])
            else:
                text += self.tweet['quoted_status']['text']
                text += self._fetch_user_mentions(self.tweet['quoted_status'])
                text += self._fetch_urls(self.tweet['quoted_status'])

        # store as member for debugging use
        self.relevant_text = text
        return text

    # private methods

    def _find_matching_keywords_for_project(self, relevant_text, config):
        """Find matching_keywords"""
        relevant_text = relevant_text.lower()
        keywords = [k.lower().split() for k in config['keywords']]
        matching_keywords = defaultdict(list)
        for keyword_list in keywords:
            if len(keyword_list) == 1:
                if keyword_list[0] in relevant_text:
                    matching_keywords[config['slug']].append(keyword_list[0])
            else:
                # keywords with more than one word: Check if all words are contained in text
                match_result = re.findall(r'{}'.format('|'.join(keyword_list)), relevant_text)
                if set(match_result) == set(keyword_list):
                    matching_keywords[config['slug']].extend(keyword_list)
        self.matching_keywords = dict(matching_keywords)

    def _match_to_config(self, relevant_text, config):
        """Match text to config in stream"""
        relevant_text = relevant_text.lower()
        match_candidates = set()
        matching_keywords_by_project = defaultdict(list)
        for c in config:
            # else find match for keywords to relevant text
            keywords = [k.lower().split() for k in c['keywords']]
            for keyword_list in keywords:
                if len(keyword_list) == 1:
                    if keyword_list[0] in relevant_text:
                        match_candidates.add(c['slug'])
                        matching_keywords_by_project[c['slug']].append(keyword_list[0])
                else:
                    # keywords with more than one word: Check if all words are contained in text
                    match_result = re.findall(r'{}'.format('|'.join(keyword_list)), relevant_text)
                    if set(match_result) == set(keyword_list):
                        match_candidates.add(c['slug'])
                        matching_keywords_by_project[c['slug']].extend(keyword_list)
        # filter by language setting
        config_dict = {c['slug']: c for c in config}
        lang_tweet = self.tweet['lang']
        candidates = set()
        for c in match_candidates:
            languages = config_dict[c]['lang']
            # add as match if language matches, no language was specified or language could not be detected by Twitter ('und')
            if lang_tweet in languages or len(languages) == 0 or lang_tweet == 'und':
                self.matching_keywords[c] = matching_keywords_by_project[c]
                candidates.add(c)
        return list(candidates)

    def _fetch_urls(self, obj):
        t = []
        if 'urls' in obj['entities']:
            for u in obj['entities']['urls']:
                t.append(u['expanded_url'])

        if 'extended_entities' in obj:
            if 'media' in obj['extended_entities']:
                for m in obj['extended_entities']['media']:
                    t.append(m['expanded_url'])
        return ''.join(t)

    def _fetch_user_mentions(self, obj):
        t = []
        if 'user_mentions' in obj['entities']:
            for user_mention in obj['entities']['user_mentions']:
                t.append(user_mention['screen_name'])
        return ''.join(t)
        
    def _get_tweet(self, tweet):
        if self.is_retweet:
            return tweet['retweeted_status']
        else:
            return tweet


    def _is_retweet(self, tweet):
        return 'retweeted_status' in tweet

