import logging
import re
import html
import unicodedata
import unidecode
from app.utils.tokenizer_contractions import CONTRACTIONS
import en_core_web_sm


nlp = en_core_web_sm.load()
logger = logging.getLogger(__name__)
control_char_regex = r'[\r\n\t]+'

def preprocess(text,
        min_num_tokens=0,
        lower_case=False,
        remove_punct=False,
        remove_accents=False,
        expand_contractions=False,
        lemmatize=False,
        remove_stop_words=False,
        replace_user_tags_with=None,
        replace_url_tags_with=None,
        ):
    """
    Main function for text preprocessing/standardization.

    Supported config:
    - min_num_tokens': Minimum number of tokens
    - lower_case: Lower casing
    - remove_punct: Remove punctuation
    - remove_accents: Asciify accents
    - expand_contractions: Expand contractions (such as he's -> he is, wouldn't -> would not, etc. Note that this may not always be correct)
    - lemmatize: Lemmatize strings
    - remove_stop_words: Remove stop words
    - replace_user_tags_with: Replace <@user> with (default: Do nothing)
    - replace_url_tags_with: Replace <url> with (default: Do nothing)
    """
    # remove HTMl symbols
    text = html.unescape(text)
    # standardize text
    text = standardize_text(text)
    # replace <@user> tags
    if isinstance(replace_user_tags_with, str):
        text = text.replace('<@user>', replace_user_tags_with)
    # replace <url> tags
    if isinstance(replace_url_tags_with, str):
        text = text.replace('<url>', replace_url_tags_with)
    # remove accents
    if remove_accents:
        text = remove_accented_chars(text)
    # expand contractions
    if expand_contractions:
        text = expand_contractions(text)
    if min_num_tokens > 0 or remove_punct or lemmatize or remove_stop_words:
        tokens = tokenize(text)
        # ignore everything below min_num_tokens
        if min_num_tokens > 0:
            num_tokens = sum((1 for t in tokens if t.is_alpha and not t.is_punct and t.text.strip()))
            if num_tokens < min_num_tokens:
                return ''
        # remove punctuation
        if remove_punct:
            tokens = [t for t in tokens if not t.is_punct]
        # remove stop words
        if remove_stop_words:
            tokens = [t for t in tokens if not t.is_stop]
        # merge
        if (remove_stop_words or remove_punct) and not lemmatize:
            text = ' '.join([t.text for t in tokens])
        if lemmatize:
            text = ' '.join([t.lemma_ for t in tokens])
    # remove duplicate whitespaces
    text = re.sub(' +', ' ', text)
    # lower casing
    if lower_case:
        text = text.lower()
    # remove trailing/leading whitespaces
    text = text.strip()
    return text

def remove_control_characters(s):
    if not isinstance(s, str):
        return s
    # replace \t, \n and \r characters by a whitespace
    s = re.sub(control_char_regex, ' ', s)
    # replace HTML codes for new line characters
    s = s.replace('&#13;', '').replace('&#10;', '')
    # removes all other control characters and the NULL byte (which causes issues when parsing with pandas)
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

def expand_contractions(text):
    contractions_pattern = re.compile('({})'.format('|'.join(CONTRACTIONS.keys())), flags=re.IGNORECASE|re.DOTALL)
    def expand_match(contraction):
        match = contraction.group(0)
        first_char = match[0]
        expanded_contraction = CONTRACTIONS.get(match)\
                if CONTRACTIONS.get(match)\
                else CONTRACTIONS.get(match.lower())
        expanded_contraction = first_char+expanded_contraction[1:]
        return expanded_contraction
    expanded_text = contractions_pattern.sub(expand_match, text)
    expanded_text = re.sub("'", "", expanded_text)
    return expanded_text

def standardize_text(text):
    """Replace some non-standard characters such as ” or ’ with standard characters. """
    transl_table = dict([(ord(x), ord(y)) for x, y in zip( u"‘’´“”–-",  u"'''\"\"--")])
    text = text.translate(transl_table)
    return text

def remove_accented_chars(text):
    """remove accented characters from text, e.g. café"""
    text = unidecode.unidecode(text)
    return text

def tokenize(text):
    # create doc
    doc = nlp(text, disable=['parser', 'tagger', 'ner'])
    # find hashtag indices and merge again (so the # are not lost)
    hashtag_pos = []
    for i, t in enumerate(doc[:-1]):
        if t.text == '#':
            hashtag_pos.append(i)
    with doc.retokenize() as retokenizer:
        for i in hashtag_pos:
            try:
                retokenizer.merge(doc[i:(i+2)])
            except ValueError:
                pass
    return [i for i in doc]
