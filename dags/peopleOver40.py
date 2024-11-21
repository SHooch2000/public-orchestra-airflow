import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd 

def peopleOver40():
    df=pd.read_json('/opt/airflow/data/fromAirflow.JSON')
    df = df[df['age'] > 40]
    print(df)
    
    df.to_csv('/opt/airflow/data/over40.csv', index=False)

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
    returnOver40 = PythonOperator(task_id='peopleOver40', python_callable=peopleOver40)

    print_ending = BashOperator(task_id='ending',
                                  bash_command='People over the age of 40 returned"')
    
    print_starting >> returnOver40 >> print_ending
    