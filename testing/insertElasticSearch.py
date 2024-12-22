from elasticsearch import Elasticsearch
from elasticsearch import helpers
from faker import Faker

fake= Faker()

# Helper action to create bulk amount of documents for NoSQL database
actions = [{
    "_index": "users",
    "_document": "doc",
    "_source": {
        "name": fake.name(),
        "street": fake.street_address(),
        "city": fake.city(),
        "zip": fake.zipcode()
    }}
    for x in range(998)]

es = Elasticsearch({'http://localhost:9200'})

 
# Single Insert
#doc={"name": fake.name(),"street": fake.street_address(), "city": fake.city(),"zip":fake.zipcode()}

# Inserting the document into the index "users" with the type "doc"
res = helpers.bulk(es, actions)

# print the word created to console.
#print(res['result']) 