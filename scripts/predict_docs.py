"""
Script to predict synced data from Elasticsearch
"""

import sys; sys.path.append('../web')
from app.utils.project_config import ProjectConfig
from app.utils.predict import Predict
import json
import os
from elasticsearch import helpers
from datetime import datetime
import logging
import argparse
import requests
import re
import boto3
import unicodedata
import time
import pickle
import fasttext
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)
control_char_regex = r'[\r\n\t]+'
label_prefix = '__label__'

def replace_urls(tweet_text, filler='<url>'):
    return re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))', filler, tweet_text)

def replace_user_mentions(tweet_text, filler='@<user>'):
    return re.sub('(?:\@)\S+', filler, tweet_text)

def remove_control_characters(s):
    if not isinstance(s, str):
        return s
    # replace \t, \n and \r characters by a whitespace
    s = re.sub(control_char_regex, ' ', s)
    # replace HTML codes for new line characters
    s = s.replace('&#13;', '').replace('&#10;', '')
    # removes all other control characters and the NULL byte (which causes issues when parsing with pandas)
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")

def process(text):
    text = replace_urls(text)
    text = replace_user_mentions(text)
    text = remove_control_characters(text)
    return text

def get_model(run_dir, run_name):
    model_path = os.path.join(run_dir, run_name, 'model.bin')
    model = fasttext.load_model(model_path)
    return model

def get_label_mapping(run_dir, run_name):
    label_mapping_path = os.path.join(run_dir, run_name, 'label_mapping.pkl')
    with open(label_mapping_path, 'rb') as f:
        label_mapping = pickle.load(f)
    return label_mapping

def labels_to_int(labels):
    """Heuristic to convert label to numeric value. Parses leading numbers in label tags such as 1_worried -> 1.
    If any conversion fails this function will return None
    """
    label_vals = []
    for label in labels:
        if label == 'positive':
            label_vals.append(1)
        elif label == 'negative':
            label_vals.append(-1)
        elif label == 'neutral':
            label_vals.append(0)
        else:
            label_split = label.split('_')
            try:
                label_val = int(label_split[0])
            except ValueError:
                return
            else:
                label_vals.append(label_val)
    return label_vals

def predict(model, label_mapping, texts, legacy=False):
    """Predict function for fasttext_v1 (legacy) model"""
    predictions = []
    for text in tqdm(texts):
        candidates = model.predict(text, k=len(label_mapping))
        probabilities = candidates[1].tolist()
        _labels = [label[len(label_prefix):] for label in candidates[0]]
        if legacy:
            _labels = [label_mapping[l] for l in _labels]
        label_vals = labels_to_int(_labels)
        predictions.append({
            'labels': _labels,
            'probabilities': probabilities,
            'label_vals': label_vals
            })
    return predictions


def main(args):
    f_path = os.path.join(args.input)
    docs = []
    num_docs = 0
    with open(f_path, 'r') as f:
        for line in f:
            doc = json.loads(line)
            try:
                docs.append({'id': doc['_id'], 'text': process(doc['_source']['text'])})
            except KeyError:
                logger.warning(f'Doc {doc} is missing text/id column')
                continue
            num_docs += 1
            if num_docs % 10000 == 0:
                logger.info(f'Loaded {num_docs:,} documents...')
    if len(docs) == 0:
        logger.info('No documents loaded.')
        return
    pc = ProjectConfig()
    session = boto3.Session(profile_name='crowdbreaks-dev')
    project_config = pc.get_config_by_index_name(args.index)
    if project_config is None:
        raise ValueError(f'Project {args.index} not found in config file.')
    predictions = {}
    if len(project_config['model_endpoints']) > 0:
        project = project_config['slug']
        texts = [t['text'] for t in docs]
        ids = [t['id'] for t in docs]
        es_index_name = project_config['es_index_name']
        for question_tag, endpoints_obj in project_config['model_endpoints'].items():
            for endpoint_name, endpoint_info in  endpoints_obj['active'].items():
                model_type = endpoint_info['model_type']
                run_name = endpoint_info['run_name']
                logger.info(f'Running predictions for run {run_name}')
                model = get_model(args.run_dir, run_name)
                label_mapping = get_label_mapping(args.run_dir, run_name)
                preds = predict(model, label_mapping, texts, legacy=(run_name=='fasttext_v1' and args.index == 'project_vaccine_sentiment'))
                for _id, _pred in zip(ids, preds):
                    if es_index_name not in predictions:
                        predictions[es_index_name] = {}
                    if _id not in predictions[es_index_name]:
                        predictions[es_index_name][_id] = {}
                    if question_tag not in predictions[es_index_name][_id]:
                        predictions[es_index_name][_id][question_tag] = {'endpoints': {}}
                    predictions[es_index_name][_id][question_tag]['endpoints'][run_name] = {
                            'label': _pred['labels'][0],
                            'probability': _pred['probabilities'][0]
                            }
                    # if present, add label vals (numeric values of labels)
                    if 'label_vals' in _pred:
                        predictions[es_index_name][_id][question_tag]['endpoints'][run_name]['label_val'] = _pred['label_vals'][0]
                    if endpoints_obj['primary'] == endpoint_name:
                        # current endpoint is primary endpoint
                        predictions[es_index_name][_id][question_tag]['primary_endpoint'] = run_name
                        predictions[es_index_name][_id][question_tag]['primary_label'] = _pred['labels'][0]
                        if 'label_vals' in _pred:
                            predictions[es_index_name][_id][question_tag]['primary_label_val'] = _pred['label_vals'][0]

    if len(predictions) > 0:
        ts = int(time.time())
        f_out = os.path.join('cache', f'predictions_{args.index}_{ts}.jsonl')
        logger.info(f'Writing predictions to file {f_out}...')
        with open(f_out, 'a') as f:
            for es_index_name, pred_es_index in predictions.items():
                for _id, pred_obj in pred_es_index.items():
                    f.write(json.dumps({
                        '_id': _id,
                        '_type': 'tweet',
                        '_op_type': 'update',
                        '_index': es_index_name,
                        '_source': {
                            'doc': {
                                'meta': pred_obj
                                }
                            }
                        }) + '\n')
    else:
        logger.info('No predictions were made. No files written.')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, type=str, help='Name of of input file')
    parser.add_argument('-p', '--index', required=True, type=str, help='Index name of project')
    parser.add_argument('-r', '--run-dir', dest='run_dir', required=True, type=str, help='Run output dir')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
