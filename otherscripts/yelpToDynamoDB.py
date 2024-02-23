import requests
import json
import boto3
from datetime import datetime
from decimal import Decimal
import time



# Initialize a Boto3 DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

for n in range(0,20):

    i = str(n * 50)
    # Make the Yelp API call
    url = "https://api.yelp.com/v3/businesses/search?location=Manhattan&term=restaurants&categories=brazil&sort_by=best_match&limit=50&offset=" + i

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer ry1POtqQx39pd9BAa_zXIpqi_IsxFuy7hZCIUzuut96ZX8k9UWLqdvs8z8qzuKVcTtdmlACh3aeY_bFVskvL7ahLRuu3iwFFs3zIJBTfLekKmriQ9bK8doyeU0XSZXYx"
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    # Example to extract a list of restaurants
    restaurants = data['businesses']

    # Extract information for each restaurant
    for restaurant in restaurants:
        response = table.put_item(
            Item={
                'BusinessID': restaurant['id'],
                'CuisineType': 'brazil',
                'Name': restaurant['name'],
                'Address': ", ".join(restaurant['location']['display_address']),
                'Coordinates': {'Latitude': str(restaurant['coordinates']['latitude']), 'Longitude': str(restaurant['coordinates']['longitude'])},
                'Number_of_Reviews': restaurant['review_count'],
                'Rating': Decimal(str(restaurant['rating'])),
                'ZipCode': restaurant['location']['zip_code'],
                'insertedAtTimestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        )

print("Now is brazil")
    
