import json
import os
import sys

os.environ["LOCAL_MODE"] = "true"
os.environ["AWS_REGION"] = "ap-southeast-1"
os.environ["DATA_LAKE_BUCKET"] = "mock-bucket"
os.environ["INPUT_PREFIX"] = "raw/input/"
os.environ["RAW_PREFIX"] = "raw/api_response/"
os.environ["BRONZE_PREFIX"] = "bronze/"
os.environ["PDL_SECRET_ARN"] = "REDACTED"
os.environ["PDL_API_KEY"] = "<INSERT KEY HERE>"

event = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "ap-southeast-1",
            "eventTime": "2026-04-01T10:15:30.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AWS:EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "203.0.113.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLEID2"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "input-bucket-trigger",
                "bucket": {
                    "name": "mock-bucket",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::my-input-bucket"
                },
                "object": {
                    "key": "raw/input/profiles.csv",
                    "size": 1234,
                    "eTag": "abcdef1234567890",
                    "sequencer": "0055AED6DCD90281E5"
                }
            }
        }
    ]
}

event2 = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "ap-southeast-1",
            "eventTime": "2026-04-01T10:20:00.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AWS:EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "203.0.113.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLEID2"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "raw-bucket-trigger",
                "bucket": {
                    "name": "mock-bucket",
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::mock-bucket"
                },
                "object": {
                    "key": "raw/api_response/date=2026-04-01/20260401T130955Z_c8316bc2.json",
                    "size": 45678,
                    "eTag": "abcdef1234567890",
                    "sequencer": "0060ABCDEF12345678"
                }
            }
        }
    ]
}

sys.path.append("src")
from handler import lambda_handler
#print('hello')

#Uncomment to test Stage 1 (change Records.s3.object.key file name in the event object to follow ur local file name)
response = lambda_handler(event, None)
#uncomment to test Stage 2 (change Records.s3.object.key file name in the event object to follow the file name that was created in the local output folder)
#response = lambda_handler(event2, None)
print(json.dumps(response, indent=2))