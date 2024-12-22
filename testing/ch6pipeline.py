import urllib.parse
import urllib.request
import requests
import json

def fetch_data_page():
    """
    Fetch data from the API and handle pagination.
    """
    try:
        # Send GET request to the SeeClickFix API
        url = "https://seeclickfix.com/api/v2/issues"
        params = {
            "place_url": "bernalillo-county",  # Adjust as needed
            "per_page": 100  # Maximum allowed issues per page
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Get the list of issues and pagination info
        issues = data['issues']
        metadata = data['metadata']['pagination']
        
        # If there's a next page, pass the URL for the next request
        next_page_url = metadata.get('next_page_url', None)
        
        # Return issues and the URL for the next page
        return issues, next_page_url
    except Exception as e:
        raise Exception(f"Error fetching data: {e}")


# The fetch_issues function queries the SeeClickFix API with specific parameters (place_url, per_page) 
# and saves the response JSON containing all issues to /tmp/issues.json.
# The split_json function splits the issues into individual files, each representing one issue.
def fetch_issues():
    try:
        base_url = "https://seeclickfix.com/api/v2/issues"
        params = {
            "place_url": "bernalillo-county",  # Adjust as needed
            "per_page": 100  # Maximum allowed issues per page
        }
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        issues = data['issues']
        
        # Save issues to a file
        with open('output.json', 'w') as f:
            json.dump({"issues": issues}, f)

        print(data['metadata'])

    except Exception as e:
        print("Error: ", e)
    

# The split_json function splits the issues into individual files, each representing one issue.
def split_json():
    try:
        with open('output.json', 'r') as f:
            data = json.load(f)
        issues = data['issues']  # Split JSON array
        for i, issue in enumerate(issues):
            with open(f'C:/Users/SidneyHoch/Documents/GitHub/public-orchestra-airflow/data/tmp/issue_{i}.json', 'w') as out_file:
                json.dump(issue, out_file)
    except Exception as e:
        print("Error: ", e)


# The process_json function processes each JSON file:
# Adds coords (a concatenation of lat and lng).
# Adds opendate (extracted from the created_at field).
def process_json(issue_index):
    with open(f'C:/Users/SidneyHoch/Documents/GitHub/public-orchestra-airflow/data/tmp/issue_{issue_index}.json', 'r') as f:
        issue = json.load(f)
    # Add 'coords' and 'opendate'
    issue['coords'] = f"{issue['lat']},{issue['lng']}"
    issue['opendate'] = issue['created_at'].split('T')[0]
    with open(f'C:/Users/SidneyHoch/Documents/GitHub/public-orchestra-airflow/data/tmp/processed_issue_{issue_index}.json', 'w') as out_file:
        json.dump(issue, out_file)


next_page_url = 'https://seeclickfix.com/api/v2/issues?page=2&place_url=bernalillo-county'

response = requests.get(next_page_url)
if response.status_code == 200:
    print("Successfully fetched data:", response.json())
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")






