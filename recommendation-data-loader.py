from elasticsearch import Elasticsearch

import random
import datetime
import argparse
import sys
import json

def get_index_path(collection_name, namespace, lcp_project_id):
    index_path = "{}_{}_{}".format(
        lcp_project_id, namespace, collection_name
    )

    return index_path.lower()

def get(es, id, index, doc_type):
    document = es.get(
        doc_type=doc_type,
        id=id,
        index=index
    )

    return document.get('_source')
    
def index(es, body, id, index, doc_type):
    body['id'] = id

    return es.index(body=body, id=id, index=index, doc_type=doc_type)

def search(es, body, index):
    response = es.search(body=body, index=index)

    search_hits = response.get('hits')

    documents = [hit.get('_source') for hit in search_hits.get('hits')]

    return documents, search_hits.get('total')

argument_parser = argparse.ArgumentParser(
    usage='{} recommendation_data_loader '
    '--command <Command> '
    '--elasticsearch-hostname <Elasticsearch Hostname> (Optional) '    
    '--model-name <model Name> '
    '--lcp-project-id <LCP Project ID>'.format(sys.argv[0])
)

argument_parser.add_argument('--command', required=True)
argument_parser.add_argument('--elasticsearch-hostname', default='http://localhost:9200')
argument_parser.add_argument('--lcp-project-id', required=True)
argument_parser.add_argument('--model-name', required=True)
argument_parser.add_argument('--output-version-id', required=False)
argument_parser.add_argument('--output-version-status', required=False, default="RUNNING")
argument_parser.add_argument('--output-version-events', required=False, default=random.randrange(1000, 10000))
argument_parser.add_argument('--output-version-items', required=False, default=random.randrange(500, 1000))

args = argument_parser.parse_args()

es = Elasticsearch(args.elasticsearch_hostname)

if args.command == 'add_output_version':
    documents, total = search(
        es,
        {
            "query": {
                "term": {
                    "name" : args.model_name
                }
            }
        }, get_index_path('jobs', 'osbasahfaroinfo', args.lcp_project_id)
    )

    if total == 0:
        sys.exit('No such model with name "{}"'.format(args.model_name))

    job = documents[0]

    now = datetime.datetime.utcnow()
    now_str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

    id = int(now.timestamp())
    job_runs_index = get_index_path('job-runs', 'osbasahfaroinfo', args.lcp_project_id)

    body = {
        'context': {},
        'createdDate': now_str,
        'job': {
            'id': job.get('id') ,
            'type': job.get('type')
        } ,
        'lastUpdatedDate': now_str,
        'status': args.output_version_status,
        'step': 'DATA_PREPARATION',
        'trigger': 'MANUAL'
    }

    if args.output_version_status in ['COMPLETED', 'PUBLISHED']:
        body['completedDate'] = now_str
        body['context']['itemsDatasetCount'] = args.output_version_items
        body['context']['userItemInteractionsDatasetCount'] = args.output_version_events
    
    index(
        es, body, id, job_runs_index, 'job-runs'
    )

    print("Output Version {}".format(get(es, id, job_runs_index, 'job-runs')))
elif args.command == 'add_assets':
    dataSourceId = '433644536177566268'

    with open("data/assets.json", "r") as f:
        for line in f:
            doc = json.loads(line)

            doc['dataSourceId'] = dataSourceId

            index(es, doc, doc.get('id'), get_index_path('assets', 'osbasahfaroinfo', args.lcp_project_id), 'assets')
elif args.command == 'add_activities':
    dataSourceId = '433644536177566268'

    with open("data/activities.json", "r") as f:
        for line in f:
            doc = json.loads(line)

            doc['dataSourceId'] = dataSourceId

            index(es, doc, doc.get('id'), get_index_path('activities', 'osbasahfaroinfo', args.lcp_project_id), 'activities')