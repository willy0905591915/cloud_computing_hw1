import boto3
import random
import json
import requests
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError

# Initialize clients
dynamodb = boto3.client('dynamodb')
ses = boto3.client('ses', region_name='us-east-1')
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    
    for record in event['Records']:
        message_body = json.loads(record['body'])
        
        cuisine = message_body['Cuisine']
        email = message_body['Email']
            
        # Query OpenSearch for a random restaurant recommendation based on CuisineType
        restaurant_id = get_random_restaurant(cuisine)
        print("restaurantID is " + restaurant_id)
        
        # Fetch restaurant details from DynamoDB using 'BusinessID'
        dynamo_response = dynamodb.get_item(
            TableName='yelp-restaurants',
            Key={'BusinessID': {'S': restaurant_id}}
        )
        restaurant_details = dynamo_response.get('Item', {})
        
        # Format email content
        email_content = format_email_content(restaurant_details)
        
        # Try to send the email
        
        # Send email
        ses_response = ses.send_email(
            Source='willy0972205402@gmail.com',  # This must be a verified email in SES
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': 'Your Restaurant Recommendation'},
                'Body': {'Text': {'Data': email_content}}
            }
        )
        print(f"Email sent! Message ID: {ses_response['MessageId']}")
        
        # Delete the message from the queue if email sent successfully
        sqs.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/058264546835/Q1', ReceiptHandle=record['receiptHandle'])
        
    return {'statusCode': 200, 'body': json.dumps('Success')}


def get_random_restaurant(cuisine):
    # OpenSearch details
    opensearch_domain_endpoint = 'https://search-restaurantnew-k73uustnh6bsnc44dkw6cykwka.us-east-1.es.amazonaws.com'
    index_name = 'restaurants'
    url = f"{opensearch_domain_endpoint}/{index_name}/_search"
    
    auth = HTTPBasicAuth('willy0905591915', '#Willy0209')
    headers = {
        'Content-Type': 'application/json',
    }
    
    # Construct the query
    query = {
        "size": 1,
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        },
        "sort": [
            {
                "_script": {
                    "script": "Math.random()",
                    "type": "number",
                    "order": "asc"
                }
            }
        ]
    }
    
    # Make the request to OpenSearch
    opensearch_response = requests.get(url, headers=headers, auth=auth, data=json.dumps(query))
    
    if opensearch_response.status_code == 200:
        # Parse the response
        response_json = opensearch_response.json()
        hits = response_json.get('hits', {}).get('hits', [])
        
        if hits:
            # Assuming your document ID is stored in _id
            return hits[0]['_source']['RestaurantID']
    return None

def format_email_content(restaurant_details):
    # Extract restaurant details from the DynamoDB response
    name = restaurant_details.get('Name', {}).get('S', 'N/A')
    address = restaurant_details.get('Address', {}).get('S', 'N/A')
    cuisine = restaurant_details.get('CuisineType', {}).get('S', 'N/A')
    rating = restaurant_details.get('Rating', {}).get('N', 'N/A')  # Assuming rating is a Number
    
    # Format the email content
    email_content = f"""
    Hello,

    Here is your personalized restaurant recommendation:

    Name: {name}
    Cuisine: {cuisine}
    Address: {address}
    Rating: {rating}
    
    Enjoy your meal!

    Best,
    Your Restaurant Recommendations Team
    """
    return email_content


