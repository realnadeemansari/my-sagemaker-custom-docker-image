import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import lambdas.dd_dl_triage_model_xgb as dd_xgb
import pytest
import os
import boto3
from moto import mock_sagemaker

region_name = "us-east-1"
os.environ["ENDPOINT_NAME"] = "test_endpoint"

@mock_sagemaker
def test_sagemaker():
    event = {"body" :{
    "Status": "no longer leaking, wet <1 day",
     "Levels": "1 floor", 
     "Rooms": "5 or more rooms", 
     "Materials": "exterior structure", 
     "Engagement": "attorney", 
     "source": "water"
    }}

    result = dd_xgb.lambda_handler(event, "")
    assert result['statusCode'] == 500