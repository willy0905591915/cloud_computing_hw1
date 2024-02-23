import json
import boto3
from datetime import datetime

# Initialize the Lex V2 client
lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        messages = body['messages']
        responses = []

        for message in messages:
            # Define parameters for Lex V2
            params = {
                "botId": "E2MB19STWI",
                "botAliasId": "86JGFRAAFQ",
                "localeId": "en_US",
                "sessionId": "unique-user-id",  # Replace with a unique ID for the user interacting with the bot
                "text": message['unstructured']['text'],
            }

            # Send the user message to Lex V2 and get the response
            try:
                lex_response = lex_client.recognize_text(**params)
                response_message = lex_response["messages"][0]["content"] if lex_response.get("messages") else "No response from Lex."
            except Exception as e:
                print(e)
                response_message = "Error communicating with Lex: " + str(e)

            # Append the Lex response to the responses list
            responses.append({
                "type": "unstructured",
                "unstructured": {
                    "id": "test-id",
                    "text": response_message,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
            })

        # Return the response messages
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"  # Needed for CORS
            },
            "body": json.dumps({"messages": responses})
        }

    except Exception as e:
        print(e)
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": "An unexpected error occurred"})
        }
