from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import time
app = Flask(__name__)
# Connect to the elastic cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

l_dataset = es.count(index='opal_dataset')['count']
l_dist = es.count(index='opal_distribution')['count']
l_measure = es.count(index='opal_measurements')['count']

def search_exact_uri(term):
    query = {"from": 0, "size": 200, 'query': {
        'multi_match': {"query": term, "fields": ['subject', 'predicate', 'object_uri']}}}

    result = es.search(index='rdf', body=query)
    total_num_of_triples = result['hits']['total']['value']

    info_occurrence = "{1} occurred in {0} number of triples".format(total_num_of_triples, term)
    return info_occurrence, result['hits']['hits']


def match_specificy(rdf_component, term):
    dquery = {"from": 0, "size": 200, 'query': {'match': {rdf_component: term}}}
    # Query
    result = es.search(index='rdf', body=dquery)
    total_num_of_triples = result['hits']['total']['value']

    info_occurrence = "{1} occurred in {0} number of triples as {2}".format(total_num_of_triples, term, rdf_component)
    return info_occurrence, result['hits']['hits']


def pose_query(term):
    info_occurrence, hits_infos = search_exact_uri(term)

    refined_results = []
    for i in hits_infos[:20]:
        id_of_t = i['_id']
        relavence_score_t = i['_score']
        s, p, o = i['_source']['subject'], i['_source']['predicate'], i['_source']['object_uri']
        refined_results.append('{0} {1} {2} .\tRelevance Score:{3}'.format(s, p, o, relavence_score_t))
    return info_occurrence, refined_results


def search_in_dcat(index_name, term, field=None):
    """

    :param term: a single term
    :param field:
    :return:
    """

    if field is None:
        field = ['http://purl.org/dc/terms/description', 'http://purl.org/dc/terms/title',
                 'http://www.w3.org/ns/dcat#keyword']

    query = {"from": 0, "size": 200,
             'query': {
                 'multi_match': {
                     "query": term,
                     "fields": field}}}

    result = es.search(index=index_name, body=query)
    info_occurrence = "{1} occurred in {0} number of triples".format(result['hits']['total']['value'], term)
    hits_infos = result['hits']['hits']

    refined_results = []
    for i in hits_infos[:20]:
        dataset_uri = i['_id']
        relavence_score_t = i['_score']
        refined_results.append('{0} .\tRelevance Score:{1}'.format(dataset_uri, relavence_score_t))

    return info_occurrence, refined_results


def preprocessing(free_text_query, desired_field):
    startT = time.time()

    if desired_field == 'dcat_distribution_domain':
        num_occurrence, results = search_in_dcat('opal_distribution', free_text_query)

    elif desired_field == 'dcat_dataset_titles':
        num_occurrence, results = search_in_dcat('opal_dataset', free_text_query,
                                                 field=['http://purl.org/dc/terms/title'])
    elif desired_field == 'dcat_dataset_description':
        num_occurrence, results = search_in_dcat('opal_dataset', free_text_query,
                                                 field=['http://purl.org/dc/terms/description'])

    elif desired_field == 'dcat_dataset_keywords':
        num_occurrence, results = search_in_dcat('opal_dataset', free_text_query,
                                                 field=['http://www.w3.org/ns/dcat#keyword'])
    elif desired_field == 'dcat_dataset_domain':
        num_occurrence, results = search_in_dcat('opal_dataset', free_text_query)
    else:
        raise NotImplementedError()

    return num_occurrence, results,str(round((time.time()-startT),3))+' seconds'


@app.route("/", methods=['POST'])
def searching():
    field = request.form.get('search_type')
    query = request.form.get('search_query')

    num_occurrence, results, query_time= preprocessing(free_text_query=query, desired_field=field)

    return render_template('index.html', query_time=query_time,search_query=query,text_val=query, search_type=field,message=num_occurrence,
                           response=results,dcat_dataset_size=l_dataset,dcat_distr_size=l_dist,measurement_size=l_measure)


@app.route("/", methods=['GET'])
def index():


    return render_template('index.html',dcat_dataset_size=l_dataset,dcat_distr_size=l_dist,measurement_size=l_measure)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
