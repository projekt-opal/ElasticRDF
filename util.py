import re
from typing import Sequence
import os
import bz2

import requests
from rdflib import Graph, URIRef
import concurrent.futures

e=concurrent.futures.ProcessPoolExecutor()

def parse_n3(sentence):
    flag = 0

    components = re.findall('<(.+?)>', sentence)

    if len(components) == 2:
        # N3 contains a literal.
        s, p = components
        remaining_sentence = sentence[sentence.index(p) + len(p) + 2:]
        literal = remaining_sentence[:-2]
        o = literal
        flag = 2

    elif len(components) == 3:
        # N3 consists of URIs.
        s, p, o = components
        flag = 3


    elif len(components) > 3:
        raise NotImplemented()

    else:
        ## This means that literal contained in RDF triple contains < > symbol
        print('Sentence:{0}\tSentence length:{1}'.format(sentence.encode(), len(sentence)))
        print('Extracted fields:{0}\tExtracted fields length:{1}'.format(components,len(components)))
        raise ValueError()

    s = re.sub("\s+", "", s)
    p = re.sub("\s+", "", p)
    o = re.sub("\s+", "", o)

    return s, p, o, flag


def get_path_knowledge_graphs(path: str) -> Sequence[str]:
    """

    :param path: str represents path of a KG or path of folder containing KGs
    :return: Sequence of paths
    """
    KGs = list()

    if os.path.isfile(path):
        KGs.append(path)
    else:
        for root, dir, files in os.walk(path):
            for file in files:
                if '.nq' in file or '.nt' in file or 'ttl' in file:
                    KGs.append(path + '/' + file)
    if len(KGs) == 0:
        print(path + ' is not a path for a file or a folder containing any .nq or .nt formatted files')
        exit(1)
    return KGs


def getreader(f_name):
    if f_name[-4:] == '.bz2':
        reader = bz2.open(f_name, "rt")
        return reader
    return open(f_name, "r")


def generator_of_reader(knowledge_graphs, rdf_parser=parse_n3, bound=None):
    """

    :param bound:
    :param knowledge_graphs:
    :param rdf_parser:
    :return:
    """
    if bound is None:
        for f_name in knowledge_graphs:
            reader = getreader(f_name)
            for sentence in reader:
                if len(sentence)>1:

                    try:
                        s, p, o, flag = rdf_parser(sentence)
                        yield s, p, o
                    except ValueError:
                        raise ValueError()


    else:

        for f_name in knowledge_graphs:
            reader = getreader(f_name)
            total_sentence = 0
            for sentence in reader:

                if total_sentence == bound: break
                total_sentence += 1

                try:
                    s, p, o, flag = rdf_parser(sentence)

                except ValueError:
                    raise ValueError()

                yield s, p, o




# Retrieve a single page and report the URL and contents
def load_url(url):
    return requests.get(url,timeout=10)

def generate_sparql_queries(*,offset,num_query):
    while num_query>0:
        url='http://opalpro.cs.upb.de:8891/sparql?default-graph-uri=&query=PREFIX+dcat%3A+%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0D%0Aconstruct+%7B%0D%0A++%3Fs+%3Fp+%3Fo.%0D%0A++%3Fo+%3Fp2+%3Fo2.%0D%0A%7D%0D%0AWHERE+%7B%0D%0A%7B%0D%0ASELECT+DISTINCT+%3Fs+WHERE+%7B%0D%0A%3Fs+a+dcat%3ADataset.%0D%0A%7D+LIMIT+1+OFFSET+'+str(offset)+'%0D%0A%7D%0D%0A%3Fs+%3Fp+%3Fo.%0D%0AOPTIONAL%7B%3Fo+%3Fp2+%3Fo2.%7D%0D%0A%7D%0D%0A&format=text%2Fplain&timeout=0&debug=on'
        offset+=1
        num_query-=1
        yield url    
        
def response_processer(url_responses):
    # TODO this function should process url_responses in paralel.
    #seed https://docs.python.org/3/library/multiprocessing.html
    doc=dict()
    dst=dict()
    m=dict()
    
    for response in url_responses:
        
        if not (response.status_code == 200):
            print('Response not successfull')
            return None
        
        doc,dst,m=graph_of_dataset(response.text,doc,dst,m)

    return doc,dst,m

def submit_generated_sparql(generated_sparql):
    futures=[]
    for url in generated_sparql:
        futures.append(e.submit(load_url,url))
        
    results = [f.result() for f in futures]
    
    return results


prefix_uri='http://projekt-opal.de/'
profiles=[prefix_uri+'dataset/',prefix_uri+'distribution/',prefix_uri+'measurement']

def graph_of_dataset(X,doc,dst,m):
    g = Graph()
    g.parse(data=X, format="n3")
    
    for t in g:
        s,p,o=str(t[0]),str(t[1]),str(t[2])
        
            
        if profiles[0] in s:
            # Important
            doc.setdefault(s, {}).setdefault(p,[]).append(o)
        elif profiles[1] in s:
            # Important
            dst.setdefault(s, {}).setdefault(p,[]).append(o)
        elif profiles[2] in s:
            m.setdefault(s, {}).setdefault(p,[]).append(o)
        else:
            """given (s,p,o), s is not expected URI."""
    
    return doc,dst,m


def gendata(index,X):
    for dataset_uri,body in X.items():
        yield {            
            "_index": index,
            "_id":dataset_uri,
            "_type": "_doc",
            "doc_as_upsert":True,
            "_source": body
            }


def elastic_query(es,q,selected_index):
    hits=es.search(index=selected_index,body=q)['hits']['hits']
    return [i['_source'] for i in hits]