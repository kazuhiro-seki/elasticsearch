'''
elasticsearch でのインデックスの例

usage:
python index.py

'''

import argparse
import csv
import os
import sys

from elasticsearch import Elasticsearch, helpers

def create_index(input, index='test_index'):

    es = Elasticsearch('http://localhost:9200')
    created = False

    # index settings
    settings = {
        "mappings": {
            "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "abstract": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "date": {
                    "type": "date",
                    "format": "yyyy",
                },
                "id": {
                    "type": "integer",
                    "index": "false"
                },
                "journal": {
                    "type": "text",
                    "index": "false"
                },
                "refs": {
                    "type": "text",
                    "index": "false"
                },
                "authors": {
                    "type": "text",
                    "index": "false"
                }
            }
        }
    }
    
    try:
        if es.indices.exists(index):
            es.indices.delete(index=index)
        es.indices.create(index=index, body=settings)
        print('Initialized index.')
        created = True
    except Exception as ex:
        print(str(ex))

        
    def generate_data():
        with open(input) as f:
            data = {'_index': index}
            for line in f:
                line = line.rstrip()
                if line == '':
                    yield data
                    data = {'_index': index}
                elif line.startswith('#year'):
                    line = line.replace('#year', '')
                    if len(line) == 4:
                        data['date'] = line
                elif line.startswith('#*'):
                    data['title'] = line.replace('#*', '')
                elif line.startswith('#index'):
                    data['_id'] = line.replace('#index', '')

    print('Indexing...')
    print(helpers.bulk(es, generate_data()))
    print('Done.')

'''
main
'''

if __name__ == '__main__':

    ap = argparse.ArgumentParser()
    ap.add_argument('--input', default='acm_output.txt')
    ap.add_argument('--index', default='acm')
    args = ap.parse_args()

    create_index(args.input, args.index)

