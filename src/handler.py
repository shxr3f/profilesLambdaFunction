import json
import urllib.parse
import traceback

from app.config import DATA_LAKE_BUCKET
from app.routes import route_s3_event


def lambda_handler(event, context):
    try:
        records = event.get("Records", [])
        if not records:
            raise ValueError("No records found in event")

        results = []

        for record in records:
            if record.get("eventSource") != "aws:s3":
                results.append({
                    "status": "ignored",
                    "reason": "unsupported_event_source",
                })
                continue

            bucket = record["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

            result = route_s3_event(bucket=bucket, key=key)
            results.append(result)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "results": results,
            }),
        }

    except Exception as e:
        traceback_str = traceback.format_exc()

        #print(traceback_str)  # 👈 shows in CloudWatch logs

        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": str(e),
                "traceback": traceback_str,  # optional (see note below)
            }),
        }