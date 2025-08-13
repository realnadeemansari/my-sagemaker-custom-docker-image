import json
import boto3
from datetime import datetime
import os
import traceback

bucket_name = os.environ["BUCKET_NAME"]
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # TODO implement
    try:
        filename = datetime.strftime(
            datetime.now(), "%Y-%m-%d-%H-%M-%S") + ".json"
        key = "data/logged-services/" + filename
        try:
            event_body = event["body"]
        except:
            event_body = event
        s3.put_object(Body=json.dumps(event_body), Bucket=bucket_name, Key=key)
        res = {}
        res['body'] = "Service logged successfully."
        res["statusCode"] = 200
        res.update({"headers": {"Content-Type": "application/json"}})
        return res
    except:
        print("#\n########\n\n")
        traceback.print_exc()
        print("\n\n########\n#")
        res = {}
        res['body'] = "Service not logged."
        res["statusCode"] = 500
        res.update({"headers": {"Content-Type": "application/json"}})
        return res
