# DAG to read in CSV Files, transform them and write them to a new CSV file.
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def cleanScooter():
    try:
        df=pd.read_csv('/opt/airflow/data/scooter.csv')

        df.drop(columns=['region_id'], inplace=True)
        df.columns =[x.lower() for x in df.columns]

        df['started_at'] = pd.to_datetime(df['started_at'], format='%m/%d/%Y %H:%M', errors='coerce')

        df.to_csv('/opt/airflow/data/cleanedScooter.csv', index=False)

    except Exception as e:
        print(f"Error in cleanScooter: {e}")


def filterScooter(fromDate = '2019-05-23', toDate = '2019-06-03'):
    try:
        df=pd.read_csv('/opt/airflow/data/cleanedScooter.csv')

        # Ensure 'started_at' is datetime
        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
        
        toFrom = df[(df['started_at'] > fromDate) & (df['started_at'] < toDate)]

        toFrom.to_csv('/opt/airflow/data/may23_jun3.csv', index=False)
        print(f"Data filtered between {fromDate} and {toDate}, saved to 'may23_jun3.csv'")

    except Exception as e:
        print(f"Error in filterScooter: {e}")


default_args = {
    'owner': 'Sidney H',
    'start_date': dt.datetime(2024, 11, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('cleanScooterData',
         default_args=default_args,
         schedule_interval=dt.timedelta(minutes=60),
            # '0 * * * *',
            ) as dag:
    
    cleanData = PythonOperator(task_id='clean',
                                python_callable = cleanScooter)
    
    selectData = PythonOperator(task_id='select',
                                python_callable = filterScooter,
                                op_kwargs={'fromDate': '2019-05-23', 'toDate': '2019-06-03'})
    
    copyFile = BashOperator(task_id='copy',
                                bash_command='cp /opt/airflow/data/may23_jun3.csv /opt/airflow/testing/scooterFiltered.csv')
    
    cleanData >> selectData >> copyFile
    

    
    