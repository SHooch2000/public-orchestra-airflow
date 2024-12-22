from elasticsearch import Elasticsearch
import json
import pandas as pd
from elasticsearch.helpers import scan

es = Elasticsearch("http://localhost:9200")

def topTen():
    # Create JSON object to send to elasticsearch
    doc = {"query": {"match_all": {}}}

    # Search the index "users" with the query "doc" and return the first 10 results
    res = es.search(index="users", body=doc, size=10)

    # Print the results
    for doc in res['hits']['hits']:
        print(doc['_source'])

    # Normalize the JSON object to a pandas dataframe
    df = pd.json_normalize(res['hits']['hits'])

    print(df)

def search():
    # Query the index "users" and match with the name "Ronald Goodman"
    doc = {"query": {"match": {"name": "Ronald Goodman"}}}

    res = es.search(index="users", body=doc)

    print(res['hits']['hits'][0]['_source'])

def filtering():
    # Get City Jennifertown
    doc = {"query": {"match": {"city": "Jennifertown"}}}

    res = es.search(index="users", body=doc, size = 10)

    print(res['hits']['hits'])

def filtering_bool():
    # Get City Jennifertown and ZIP 82256
    doc = {"query": {"bool": {"must": [{"match": {"city": "Jennifertown"}}, {"match": {"zip": "82256"}}]}}}

    res = es.search(index="users", body=doc, size = 10)

    print(res['hits']['hits'])

def scrolling():
    # Scroll through all the documents in the index "users"
    doc = {"query": {"match_all": {}}}

    for hit in scan(es, index="users", query=doc):
        print(hit['_source'])

    


scrolling()