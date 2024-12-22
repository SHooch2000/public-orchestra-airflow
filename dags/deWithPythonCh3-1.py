import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd 

# Reads data.csv and returns the csv into JSON format, printing only the name out in the process.
def CSVToJson():
    df=pd.read_csv('/opt/airflow/data/data.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.JSON',orient='records')

default_args = {
    # Owner as shown in Airflow GUI
    'owner' : 'Sidney H',
    # Start Date of Pipeline
    'start_date' : dt.datetime(2024,11,15),
    # Num of Retries
    'retries' : 1,
    # Delay in case of failure
    'retry_delay' : dt.timedelta(minutes=5)
}

with DAG('MyCsvDAG',
         default_args = default_args,
         schedule_interval = dt.timedelta(minutes=5),
         # '0 * * * *',
) as dag:
    
    print_starting = BashOperator(task_id='starting',
                     bash_command='echo "I am reading the CSV now....."')
    CSVJson = PythonOperator(task_id='convertCSVtoJson',
                             python_callable=CSVToJson)

    print_starting >> CSVJson


