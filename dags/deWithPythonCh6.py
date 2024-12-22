import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import requests
import json
from datetime import timedelta

# Initialize Elasticsearch client
es = Elasticsearch("http://host.docker.internal:9200", timeout=600)

# Set up logging
logger = logging.getLogger(__name__)


def fetch_data_page(url):
    """
    Fetch data from the API with pagination.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Log the URL and the status code of the response
        logger.info(f"Fetching data from URL: {url} - Status Code: {response.status_code}")
        
        data = response.json()
        issues = data['issues']
        metadata = data['metadata']['pagination']
        next_page_url = metadata.get('next_page_url', None)
        
        # Log the number of issues and next page URL
        logger.info(f"Fetched {len(issues)} issues. Next page URL: {next_page_url}")
        
        return issues, next_page_url
    
    except Exception as e:
        logger.error(f"Error fetching data from URL {url}: {e}")
        raise


def process_issues(issues):
    """
    Process the issues (e.g., extract coordinates, add fields).
    """
    for issue in issues:
        issue['coords'] = f"{issue['lat']},{issue['lng']}"
        d = issue['created_at'].split('T')
        issue['opendate'] = d[0]
    return issues


def push_to_elasticsearch(issues):
    """
    Push the processed issues to Elasticsearch.
    """
    actions = []
    for issue in issues:
        doc_id = issue['id']
        action = {
            "_op_type": "index",
            "_index": "scf",
            "_id": doc_id,
            "_source": issue
        }
        actions.append(action)
    try:
        helpers.bulk(es, actions)
        logger.info(f"Successfully indexed {len(issues)} issues.")
    except Exception as e:
        logger.error(f"Error indexing bulk data to Elasticsearch: {e}")


def fetch_next_page_and_process(url, **kwargs):
    """
    Fetch the next page of data, process issues, and handle pagination.
    """
    issues, next_page_url = fetch_data_page(url)
    processed_issues = process_issues(issues)
    push_to_elasticsearch(processed_issues)

    if next_page_url:
        logger.info(f"Next page URL found: {next_page_url}")
        kwargs['ti'].xcom_push(key='next_page_url', value=next_page_url)
        return 'continue_pagination_task'
    else:
        logger.info("No next page URL found. Ending pagination.")
        return 'end_pagination_task'

def continue_pagination(**kwargs):
    """
    Continue pagination by checking if there is a next page URL.
    """
    next_page_url = kwargs['ti'].xcom_pull(key='next_page_url', task_ids='fetch_data_page_task')

    if next_page_url:
        logger.info(f"Continuing pagination with next page URL: {next_page_url}")
        
        try:
            response = requests.get(next_page_url)

            if response.status_code == 200:
                logger.info(f"Successfully fetched data from: {next_page_url}")
                logger.info(f"Next page URL: {next_page_url}")
                if next_page_url:
                    kwargs['ti'].xcom_push(key='next_page_url', value=next_page_url)
                    # Explicitly set dependencies or trigger next task
                    return 'fetch_data_page_task'  # Continue with the next page
                else:
                    logger.info("No next page found. Ending pagination.")
                    return 'end_pagination_task'
            else:
                logger.error(f"Failed to fetch data from: {next_page_url}. Status Code: {response.status_code}")
                return 'end_pagination_task'  # End pagination in case of failure
        except Exception as e:
            logger.error(f"Error during pagination: {e}")
            return 'end_pagination_task'  # End pagination in case of exception
    else:
        logger.info("No next page URL found. Ending pagination.")
        return 'end_pagination_task'  # No more pages to process


def end_pagination():
    """
    End the pagination process if no more pages exist.
    """
    logger.info("Pagination complete. No more pages to fetch.")


# DAG setup
with DAG(
    'see_click_fix_with_pagination',
    default_args={
        'owner': 'Sidney H',
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    description='Fetch and process issues from SeeClickFix API with pagination',
    schedule_interval=timedelta(hours=12),
    start_date=days_ago(1),
) as dag:

    # Initial task to fetch the first page
    fetch_data_page_task = PythonOperator(
        task_id='fetch_data_page_task',
        python_callable=fetch_next_page_and_process,
        op_args=['https://seeclickfix.com/api/v2/issues?page=1&place_url=bernalillo-county']
    )

    continue_pagination_task = PythonOperator(
        task_id='continue_pagination_task',
        python_callable=continue_pagination
    )

    end_pagination_task = EmptyOperator(task_id='end_pagination_task')

    # Set up task dependencies
    fetch_data_page_task >> continue_pagination_task >> end_pagination_task
