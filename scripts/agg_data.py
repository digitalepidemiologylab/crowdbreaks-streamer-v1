"""
Script to run aggregation query on Elasticsearch
"""

from utils import get_es_client, add_agg_query_args, build_agg_query, ArgParseDefault
import logging
import pandas as pd
import time
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

def load_docs(args, es):
    query = build_agg_query(interval=args.interval, since=args.since, until=args.until, missing_field=args.missing_field, has_field=args.has_field, match_phrase=args.match_phrase, lang=args.lang)
    res = es.es.search(index=args.index, body=query, filter_path=['aggregations.counts'], request_timeout=60)
    res = res.get('aggregations', {}).get('counts', {}).get('buckets', [])
    return res

def main(args):
    es = get_es_client(env=args.es)
    resp = load_docs(args, es)
    df = pd.DataFrame(resp)
    df['timestamp'] = pd.to_datetime(df['key_as_string'])
    df = df.drop(['key', 'key_as_string'], axis=1)
    if args.output_file is None:
        ts = int(time.time())
        f_out = os.path.join('cache', f'agg_data_{args.index}_{ts}.csv')
    else:
        f_out = os.path.join('cache', args.output_file)
    logger.info(f'Writing fiile {f_out}...')
    df.to_csv(f_out, index=False)

def parse_args():
    parser = ArgParseDefault()
    parser.add_argument('-i', '--index', required=True, type=str, help='Name of index')
    parser.add_argument('-d', '--doc-type', dest='doc_type', default='tweet', required=False, type=str, help='Doc type')
    parser.add_argument('-o', '--out', choices=['csv'], required=False, default='csv', type=str, help='Output format')
    parser.add_argument('--output-file', dest='output_file', required=False, default=None, type=str, help='Output filename')
    parser.add_argument('--es', choices=['dev', 'stg', 'prd'], required=False, default='prd', type=str, help='Elasticsearch cluster')
    parser = add_agg_query_args(parser)
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    main(args)
