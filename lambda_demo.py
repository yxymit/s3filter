import boto3
import json

# Create a Boto3 Lambda client
lambda_client = boto3.client('lambda', region_name='us-east-2')

# Specify the name of Lambda function
function_name = 'first_demo'

# Input data for your Lambda function (if needed)
payload = {
    'key1': 'value1',
    'key2': 'value2'
}

# Invoke the Lambda function
response = lambda_client.invoke(
    FunctionName=function_name,
    InvocationType='RequestResponse',  # Use 'Event' for asynchronous invocation
    Payload=json.dumps(payload),
)

# Extract the response from the Lambda function
response_payload = json.loads(response['Payload'].read().decode('utf-8'))
print(response_payload)
