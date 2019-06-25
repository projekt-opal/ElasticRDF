import requests
import json

# Retrieve a single page and report the URL and contents
def load_url(url):
    return requests.get(url,timeout=10)

def gen_data_provider(text,index_name):
    try:
        l=iter(text.splitlines())
        while True:
            index,body=json.loads(next(l)),json.loads(next(l))

            yield {
            "_index": index_name,
            "_id":index['index']['_id'],
            "_type": "document",
            "_op_type": "update",
            "doc_as_upsert":True,
            "_source": body
            }
    except:
        """ do notthing """
        
        
def get_sample_data(url,index_name='dummy'):
    response=load_url(url)

    return gen_data_provider(response.text,index_name)
