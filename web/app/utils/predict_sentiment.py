from nltk import TweetTokenizer
import fastText
import logging
import re
import os


class PredictSentiment:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def predict(self, text, model='fasttext_v1.ftz', num_classes=3):
        text = self.tokenize(text)
        if text is None:
            return
        model_path = os.path.join(os.path.abspath('.'), 'bin', 'vaccine_sentiment', model)
        m = fastText.load_model(model_path)
        pred = m.predict(text, k=num_classes)
        label_dict = {'__label__-1': ['anti-vaccine', -1], '__label__0': ['neutral', 0], '__label__1': ['pro-vaccine', 1]}
        return {'labels': [label_dict[l][0] for l in pred[0]], 'label_vals': [label_dict[l][1] for l in pred[0]],
                'probabilities': list(pred[1]), 'model': model.split('.')[0]}

    def tokenize(self, text, discard_word_length=2):
        """Tokenize text for sentence embedding

        :discard_word_length: Discard tweets with less words than this
        :returns: Same tweet with text_tokenized field. Returns None if tweet is invalid.
        """
        tknzr = TweetTokenizer()
        # Replace unnecessary spacings/EOL chars
        try:
            text = text.replace('\n', '').replace('\r', '').strip()
        except:
            return None
        text = tknzr.tokenize(text)
        # throw away anything below certain words length
        if not discard_word_length < len(text) < 110:
            return None
        text = ' '.join(text)
        text = text.lower()
        # replace urls and mentions
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))','<url>',text)
        text = re.sub('(\@[^\s]+)','<user>',text)
        try:
            text = text.decode('unicode_escape').encode('ascii','ignore')
        except:
            pass
        filter(lambda word: ' ' not in word, text)
        return text.strip()
