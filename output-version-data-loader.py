from common import ElasticsearchBridge

import random
import datetime
import argparse
import sys
import json

argument_parser = argparse.ArgumentParser(
    usage='{} output_version_data_loader '
    '--command <Command> (add or update) -> default add '
    '--elasticsearch-hostname <Elasticsearch Hostname, Optional> -> default http://localhost:9200 '
    '--lcp-project-id <LCP Project ID> ' 
    '--model-name <model Name> '
    '--output-version-id <ID of the output version>, required for update command '
    '--output-version-status <Status of the output version>, [COMPLETED, FAILED, PUBLISHED, RUNNING] '
    '--output-version-events <Number of events of the output version>, [COMPLETED OR PUBLISHED] status -> default random number '
    '--output-version-items <Number of items of the output version>, [COMPLETED OR PUBLISHED] status -> default random number'.format(sys.argv[0])
)

def date_str():
    now = datetime.datetime.utcnow()

    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'

argument_parser.add_argument('--command', required=True, default='add')
argument_parser.add_argument('--elasticsearch-hostname', default='http://localhost:9200')
argument_parser.add_argument('--lcp-project-id', required=True)
argument_parser.add_argument('--model-name', required=True)
argument_parser.add_argument('--output-version-created-date', required=False, default=date_str())
argument_parser.add_argument('--output-version-completed-date', required=False, default=date_str())
argument_parser.add_argument('--output-version-id', required=False)
argument_parser.add_argument('--output-version-status', required=False, default="RUNNING")
argument_parser.add_argument('--output-version-events', required=False, default=random.randrange(1000, 10000))
argument_parser.add_argument('--output-version-items', required=False, default=random.randrange(500, 1000))

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

if args.command == 'add':
    id = random.randrange(9999999999999999)

    print(args.output_version_created_date)

    body = {
        'id': str(id),
        'context': {},
        'createdDate': args.output_version_created_date,
        'job': {
            'id': job.get('id') ,
            'type': job.get('type')
        } ,
        'lastUpdatedDate': args.output_version_created_date,
        'status': args.output_version_status,
        'step': 'DATA_PREPARATION',
        'trigger': 'MANUAL'
    }

    if args.output_version_status in ['COMPLETED', 'PUBLISHED']:
        body['completedDate'] = args.output_version_completed_date
        body['lastUpdatedDate'] = args.output_version_completed_date
        body['context']['itemsDatasetCount'] = args.output_version_items
        body['context']['userItemInteractionsDatasetCount'] = args.output_version_events
    
    elasticsearch_bridge.index(
        body, id, 'job-runs', 'osbasahfaroinfo'
    )

    print("Output Version {}".format(elasticsearch_bridge.get(id, 'job-runs', 'osbasahfaroinfo')))
elif args.command == 'update':
    job_run = elasticsearch_bridge.get(args.output_version_id, 'job-runs', 'osbasahfaroinfo')

    job_run['status'] = args.output_version_status.upper()

    if args.output_version_status.upper() in ['COMPLETED', 'PUBLISHED']:
        job_run['completedDate'] = args.output_version_completed_date
        job_run['lastUpdatedDate'] = args.output_version_completed_date
        job_run['context']['itemsDatasetCount'] = args.output_version_items
        job_run['context']['userItemInteractionsDatasetCount'] = args.output_version_events

    elasticsearch_bridge.update_document(job_run, job_run['id'], 'job-runs', 'osbasahfaroinfo')