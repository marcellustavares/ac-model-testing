from common import ElasticsearchBridge

import random
import datetime
import argparse
import sys
import json

argument_parser = argparse.ArgumentParser(
    usage='{} api_data_loader '
    '--elasticsearch-hostname <Elasticsearch Hostname, Optional> -> default http://localhost:9200 '
    '--lcp-project-id <LCP Project ID> ' 
    '--model-name <model Name> '.format(sys.argv[0])
)

argument_parser.add_argument('--elasticsearch-hostname', default='http://localhost:9200')
argument_parser.add_argument('--lcp-project-id', required=True)
argument_parser.add_argument('--model-name', required=True)

args = argument_parser.parse_args()

elasticsearch_bridge = ElasticsearchBridge(
    args.elasticsearch_hostname, args.lcp_project_id
)

documents, total = elasticsearch_bridge.search(
    {
        "query": {
            "term": {
                "name" : args.model_name
            }
        }
    }, 'jobs', 'osbasahfaroinfo'
)

if total == 0:
    sys.exit('No such model with name "{}"'.format(args.model_name))

job = documents[0]
job_id = job['id']

try:
    elasticsearch_bridge.delete_by_query(
        {
        "query": {
                "match_all" : {}
            } 
        }, 'recommended-items', 'osbasahfaroinfo'
    )
except Exception as e:
    pass

canonicalUrls = []

with open("data/assets.json", "r") as f:
    for line in f:
        canonicalUrls.append(json.loads(line)['canonicalUrl'])

for canonicalUrl in canonicalUrls:
    recommendations = random.choices(canonicalUrls, k=random.randrange(5, 10))

    if canonicalUrl in recommendations:
        recommendations.remove(canonicalUrl)

    id = random.randrange(9999999999999999)

    body = {
        'id': str(id),
        'itemId': canonicalUrl,
        'jobId': job_id ,
        'recommendedItemIds': recommendations 
    }

    elasticsearch_bridge.index(
        body, id, 'recommended-items', 'osbasahfaroinfo'
    )