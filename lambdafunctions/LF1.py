import json
import boto3

# Initialize boto3 client for SQS at the top of your Lambda function
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/058264546835/Q1'

def lambda_handler(event, context):
    # Extract the intent information from the Lex event
    if event['sessionState']['intent']['state'] == 'ReadyForFulfillment':
        current_intent = event['sessionState']['intent']['name']
    
        # Handle the DiningSuggestionsIntent intent
        if current_intent == 'DiningSuggestionsIntent':
            # Extract slot values from the Lex event
            slots = event['sessionState']['intent']['slots']
            location = slots['Location']['value']['interpretedValue']
            cuisine = slots['Cuisine']['value']['interpretedValue']
            numberOfPeople = slots['NumberOfPeople']['value']['interpretedValue']
            diningDate = slots['Date']['value']['interpretedValue']
            diningTime = slots['Time']['value']['interpretedValue']
            email = slots['Email']['value']['interpretedValue']
    
            # Construct the message with slot values
            message = {
                'Location': location,
                'Cuisine': cuisine,
                'NumberOfPeople': numberOfPeople,
                'Date': diningDate,
                'Time': diningTime,
                'Email': email
            }
    
            # Send the message to SQS
            response = sqs.send_message(
                QueueUrl='https://sqs.us-east-1.amazonaws.com/058264546835/Q1',
                MessageBody=json.dumps(message)
            )
            
            if 'MessageId' in response:
                return close(event['sessionState']['sessionAttributes'], "Your request has been received and you will be notified over email once I got the list of restaurant suggestions.")
            else:
            # If there was an error, close the intent as failed
                return close(event['sessionState']['sessionAttributes'], "There was a problem processing your request.", fulfillment_state="Failed")
    else:
        # If the intent is not ready for fulfillment, delegate back to Lex
        return delegate(event['sessionState']['sessionAttributes'], event['sessionState']['intent']['slots'])
        

def close(session_attributes, message, fulfillment_state="Fulfilled"):
    # Construct a response object with the `Close` dialog action
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'DiningSuggestionsIntent',
                'state': fulfillment_state
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }

def delegate(session_attributes, slots):
    # Construct a response object with the `Delegate` dialog action
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': {
                'type': 'Delegate'
            },
            'intent': {
                'slots': slots
            }
        }
    }