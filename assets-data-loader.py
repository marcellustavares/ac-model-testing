from common import ElasticsearchBridge

import random
import datetime
import argparse
import sys
import json

argument_parser = argparse.ArgumentParser(
    usage='{} assets_data_loader '
    '--elasticsearch-hostname <Elasticsearch Hostname, Optional> -> default http://localhost:9200 '
    '--lcp-project-id <LCP Project ID>'.format(sys.argv[0])
)

argument_parser.add_argument('--elasticsearch-hostname', default='http://localhost:9200')
argument_parser.add_argument('--lcp-project-id', required=True)

args = argument_parser.parse_args()

elasticsearch_bridge = ElasticsearchBridge(
    args.elasticsearch_hostname, args.lcp_project_id
)

with open("data/assets.json", "r") as f:
    for line in f:
        doc = json.loads(line)

        elasticsearch_bridge.index(
            doc, doc.get('id'), 'assets', 'osbasahfaroinfo'
        )