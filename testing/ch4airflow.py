import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def queryPostgressql():
    conn_string = "host=localhost port=5432 dbname=dataengineering user=postgres password=password connect_timeout=10 sslmode=prefer"
    conn = db.connect(conn_string)

    df = pd.read_sql("select name, city from people", conn)

    df.to_csv('./postgresqldata.csv')

    print("------Data Saved------")


def insertElasticsearch():
    es = Elasticsearch("http://localhost:9200")

    df = pd.read_csv('./data/postgresqldata.csv')

    for i, r in df.iterrows():
        doc = {
            "name": r['name'],
            "city": r['city']
        }

        res = es.index(index='frompostgresql', body=doc)

        print(res)

queryPostgressql()