"""
Script to sync data between two Elasticsearch indices. Note that the indices must have a suitable mapping
"""

from utils import get_es_client
import json
import os
from elasticsearch import helpers
from datetime import datetime, timezone
import logging
import argparse
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-5.5s] [%(name)-12.12s]: %(message)s')
logger = logging.getLogger(__name__)

def load_docs(es, args):
    query_conditions = []
    if args.since is not None or args.until is not None:
        query_conditions.append({'range': {'created_at': {'gte': parse_date(args.since), 'lte': parse_date(args.until)}}})
    query = {'query': {'bool': {'must': query_conditions}}}
    if args.query_missing_field is not None:
        query['query']['bool']['must_not'] = {'exists': {'field': args.query_missing_field}}
    if len(args.fields) > 0:
        query['_source'] = args.fields
    logger.info(f'Running query:\n{query}')
    scan = helpers.scan(es.es, scroll='1m', query=query, index=args.index, doc_type=args.doc_type, request_timeout=60)
    for item in scan:
        yield {'_index': args.index, '_type': args.doc_type, '_id': item['_id'], '_source': item['_source']}

def main(args):
    es_source = get_es_client(env='prd')
    if not es_source.test_connection():
        raise Exception('Connection issue')

    # bulk index on development
    if args.out == 'es-dev':
        es_dev = get_es_client(env='dev')
        try:
            resp = helpers.bulk(es_dev.es, load_docs(es_source, args))
        except Exception as e:
            logger.error('Bulk indexing not unsuccessful!')
            print(e)
        else:
            logger.info(f'Successfully indexed {resp[0]:,} docs...')
    elif args.out == 'jsonl':
        ts = int(time.time())
        f_out = os.path.join('cache', f'sync_es_data_{args.index}_{ts}.jsonl')
        num_rows = 0
        with open(f_out, 'w') as f:
            for line in load_docs(es_source, args):
                f.write(json.dumps(line) + '\n')
                num_rows += 1
                if num_rows % int(1e4) == 0:
                    logger.info(f'Loaded {num_rows:,} documents...')

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--index', required=True, type=str, help='Name of index')
    parser.add_argument('-d', '--doc-type', dest='doc_type', default='tweet', required=False, type=str, help='Doc type')
    parser.add_argument('-s', '--since', default=None, required=False, type=str, help='Since (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('-u', '--until', default=None, required=False, type=str, help='Until (Format: YYYY-mm-dd HH:MM:SS)')
    parser.add_argument('-o', '--out', choices=['es-dev', 'jsonl'], required=False, default='es-dev', type=str, help='Output format')
    parser.add_argument('--fields', default=[], nargs='+', help='Get only certain fields')
    parser.add_argument('--query-missing-field', dest='query_missing_field', default=None, required=False, type=str, help='Query docs with field missing')
    args = parser.parse_args()
    return args

def parse_date(date, input_format='%Y-%m-%d %H:%M:%S', output_format='%a %b %d %H:%M:%S %z %Y'):
    if isinstance(date, str) and 'now' in date:
        return date
    try:
        date = datetime.strptime(date, input_format)
    except:
        logger.warning(f'Could not parse date {date}. Fallback to "now"')
        return 'now'
    date = date.replace(tzinfo=timezone.utc)
    date = date.strftime(output_format)
    return date

if __name__ == "__main__":
    args = parse_args()
    main(args)
