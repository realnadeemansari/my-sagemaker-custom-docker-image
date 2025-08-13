import json
import boto3
import os


def lambda_handler(event, context):
    try:
        runtime = boto3.Session().client('sagemaker-runtime')
        endpoint_name = os.environ["ENDPOINT_NAME"]
        response = runtime.invoke_endpoint(EndpointName=endpoint_name,  # The name of the endpoint we created
                                        ContentType='application/json',  # The data format that is expected
                                        Body=event["body"])
        res = response["Body"].read().decode('utf-8')

        return {
            "statusCode": 200,
            "body": res,
            "headers": {'Access-Control-Allow-Origin': '*'}
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": {"message": "Error while getting the prediction from endpoints."},
            "headers": {'Access-Control-Allow-Origin': '*'}
        }