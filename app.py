from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

app = Flask(__name__)
# Connect to the elastic cluster
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def search_exact_uri(term):
    query = {"from": 0, "size": 200, 'query': {
        'multi_match': {"query": term, "fields": ['subject','predicate','object_uri']}}}

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


def search_in_dcatDataset(term,field=['http://purl.org/dc/terms/description','http://purl.org/dc/terms/title']):
    """

    :param term: a single term
    :param field:
    :return:
    """

    query = {"from": 0, "size": 200, 'query': {
        'multi_match': {
            "query": term,
            "fields": field}}}

    result = es.search(index='opal_dataset', body=query)
    info_occurrence = "{1} occurred in {0} number of triples".format(result['hits']['total']['value'], term)
    hits_infos= result['hits']['hits']

    refined_results = []
    for i in hits_infos[:20]:
        dataset_uri = i['_id']
        relavence_score_t = i['_score']
        refined_results.append('{0} .\tRelevance Score:{1}'.format(dataset_uri, relavence_score_t))


    return info_occurrence, refined_results


@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        query_term = request.form.get('text_search')


        info_occurrence, refined_results = search_in_dcatDataset(query_term)
        return render_template('index.html', text_val=query_term, message=info_occurrence, response=refined_results)

    elif request.method == 'GET':
        return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True, port=5000)
