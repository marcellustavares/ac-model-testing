from elasticsearch import Elasticsearch

class ElasticsearchBridge:
    def __init__(self, hostname, lcp_project_id):
        self.es = Elasticsearch(
		    hostname
		)
        self.lcp_project_id = lcp_project_id

    def get_index_path(self, collection_name, namespace):
        index_path = "{}_{}_{}".format(
            self.lcp_project_id, namespace, collection_name
        )

        return index_path.lower()

    def get(self, id, collection_name, namespace):
        document = self.es.get(
            doc_type=collection_name,
            id=id,
            index=self.get_index_path(collection_name, namespace)
        )

        return document.get('_source')
        
    def index(self, body, id, collection_name, namespace):
        body['id'] = id

        return self.es.index(
            body=body, id=id, 
            index=self.get_index_path(collection_name, namespace),
            doc_type=collection_name
        )

    def search(self, body, collection_name, namespace):
        response = self.es.search(
            body=body, index=self.get_index_path(collection_name, namespace)
        )

        search_hits = response.get('hits')

        documents = [hit.get('_source') for hit in search_hits.get('hits')]

        return documents, search_hits.get('total')

    def update_document(self, document, id, collection_name, namespace):
        return self.es.update(
            body={'doc': document},
            doc_type=collection_name,
            id=id,
            index=self.get_index_path(collection_name, namespace)
        )

    def delete_by_query(self, body, collection_name, namespace):
        return self.es.delete_by_query(
            body=body,
            index=self.get_index_path(collection_name, namespace)
        )