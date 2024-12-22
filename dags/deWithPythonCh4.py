# Dag to combine two python operators to extract data from postgres and write to a csv file.
# Read in the csv file and write it to ElasticSearch.

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# 192.168.1.107 IP address of local machine to connect to postgresql. Using host.docker.internal to connect to host machine from container.
def queryPostgressql():
    conn_string = "host=host.docker.internal port=5432 dbname=dataengineering user=postgres password=password connect_timeout=10"
    conn = db.connect(conn_string)

    df = pd.read_sql("select name, city from people", conn)

    df.to_csv('/opt/airflow/data/postgresqldata.csv')

    print("------Data Saved------")


def insertElasticsearch():
    es = Elasticsearch("http://host.docker.internal:9200", timeout=600)

    df = pd.read_csv('/opt/airflow/data/postgresqldata.csv')

    for i, r in df.iterrows():
        doc = {
            "name": r['name'],
            "city": r['city']
        }

        res = es.index(index='frompostgresql', body=doc)

        print(res)


default_args = {
    'owner': 'Sidney H',
    'start_date': dt.datetime(2024, 11, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('dbToElasticsearchDAG',
         default_args=default_args,
         schedule_interval=dt.timedelta(minutes=60),
            # '0 * * * *',
            ) as dag:
    
    getData = PythonOperator(task_id='QueryPostgreSQL',
                             python_callable = queryPostgressql)
    
    insertData = PythonOperator(task_id='insertElasticSearch',
                                 python_callable = insertElasticsearch)
    
    getData >> insertData





    