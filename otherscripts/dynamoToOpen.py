import boto3
import requests
import json
from requests.auth import HTTPBasicAuth

# Initialize a boto3 DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Specify the table name
table_name = 'yelp-restaurants'
table = dynamodb.Table(table_name)

# Scan the table (Note: Scan is expensive and not recommended for large tables)
response = table.scan()

# OpenSearch details
opensearch_domain_endpoint = 'https://search-restaurantnew-k73uustnh6bsnc44dkw6cykwka.us-east-1.es.amazonaws.com'  # Replace with your OpenSearch endpoint
index_name = 'restaurants'

auth = HTTPBasicAuth('willy0905591915', '#Willy0209')

headers = {
    'Content-Type': 'application/json',
}

# Count how many documents uploaded
count = 0

# Iterate over the items returned by the scan
for item in response['Items']:
    # Extract the BusinessID and CuisineType
    business_id = item['BusinessID']
    cuisine_type = item['CuisineType']

    # Construct the data to send to OpenSearch
    data = {
        'RestaurantID': business_id,
        'Cuisine': cuisine_type
    }

    # The URL for the OpenSearch index (no type specified as per new versions)
    url = f'{opensearch_domain_endpoint}/{index_name}/_doc'

    # POST the data to OpenSearch
    opensearch_response = requests.post(url, headers=headers, auth=auth, data=json.dumps(data))
    
    # Add one document count
    count += 1

print(f'Stored :{count} documents')

# Note: For production use, it's better to use the DynamoDB Streams feature to capture changes to the table and update OpenSearch accordingly.
