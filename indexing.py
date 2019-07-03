from elasticsearch import Elasticsearch
import util as ut
from elasticsearch import helpers
import time

# Connect to the elastic cluster
es=Elasticsearch([{'host':'localhost','port':9200}])

dcat_dataset_mappings = {
    "mappings": {
        "dynamic": "false",
        "properties": {

            "http://purl.org/dc/terms/title": {"type": "text"},
            "http://purl.org/dc/terms/description": {"type": "text"},
            "http://www.w3.org/ns/dcat#keyword": {"type": "text"},
            "http://purl.org/dc/terms/issued": {"type": "text"},
            "http://purl.org/dc/terms/modified": {"type": "text"},
            "http://purl.org/dc/terms/publisher": {"type": "keyword"},
            "http://xmlns.com/foaf/0.1/page": {"type": "keyword"},
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": {"type": "keyword"},
            "http://www.w3.org/ns/dqv#hasQualityMeasurement": {"type": "keyword"},
            "http://www.w3.org/ns/dcat#distribution": {"type": "keyword"},
            "http://purl.org/dc/terms/accrualPeriodicity": {"type": "keyword"},
            "http://purl.org/dc/terms/hasPart'": {"type": "text"},
            "http://www.w3.org/ns/prov#wasUsedBy": {"type": "text"},
            "http://purl.org/dc/terms/identifier": {"type": "keyword"},
            "http://xmlns.com/foaf/0.1/isPrimaryTopicOf": {"type": "text"},
            "http://purl.org/dc/terms/spatial": {"type": "text"},
            "http://purl.org/dc/terms/provenance": {"type": "text"},

        }
    }
}

dcat_measurements_mappings = {
    "mappings": {
        "dynamic": "false",
        "properties": {
            "http://www.w3.org/ns/dqv#isMeasurementOf": {"type": "keyword"},
            "http://www.w3.org/ns/dqv#value": {"type": "text"},
        }
    }
}

dcat_distribution_mappings = {
    "mappings": {
        "dynamic": "false",
        "properties": {
            "http://purl.org/dc/terms/title": {"type": "text"},
            "http://purl.org/dc/terms/description": {"type": "text"},
            "http://purl.org/dc/terms/issued": {"type": "text"},
            "http://purl.org/dc/terms/modified": {"type": "text"},
            "http://www.w3.org/ns/dcat#keyword": {"type": "text"},
            "http://www.w3.org/ns/dcat#accessURL": {"type": "keyword"},
        }
    }
}

es.indices.create(index='opal_dataset', body=dcat_dataset_mappings)
es.indices.create(index='opal_distribution', body=dcat_distribution_mappings)
es.indices.create(index='opal_measurements', body=dcat_measurements_mappings)

# Offset required for perform SPARQL query for http://opalpro.cs.upb.de:8891/sparql
offset = 0
# At each iteration num_of_sparql_query number of SPARQL query with consecutive offset is performed.
num_of_sparql_query = 1000
for _ in range(50):
    print('Size of Dcat:Dataset {0}'.format(es.count(index='opal_dataset')['count']))

    sparqls = ut.generate_sparql_queries(offset=offset, num_query=num_of_sparql_query)
    offset += num_of_sparql_query

    results = ut.submit_generated_sparql(sparqls)

    # number of queries must match
    assert len(results) == num_of_sparql_query

    try:
        document_index, dist_index, measures_index = ut.response_processer(results)
    except TypeError as tyerr:
        print('Response not successful')
        print("error: {0}".format(tyerr))
        break

    try:
        helpers.bulk(es, ut.gendata('opal_dataset', document_index))
        helpers.bulk(es, ut.gendata('opal_distribution', dist_index))
        helpers.bulk(es, ut.gendata('opal_measurements', measures_index))
    except:
        """
        During indexing we observed that using dcat:Dataset URI as ID in elasticsearch caused the following error.
        RequestError(400, 'action_request_validation_exception', 'Validation Failed: 1: id is too long, must be no longer than 512 bytes but was: 653;2
        Currently we ignored such errors.
        
        Currently 759392 # of dcat:Dataset is available at our virtuoso instance.
        As a result of such indexing issue we did not index 2000 dcat:Dataset. Hence we index 757392 documents
        """
        continue