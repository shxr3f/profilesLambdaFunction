import json

from app.config import DATA_LAKE_BUCKET, INPUT_KEY, PDL_API_KEY
from app.processor import process_profiles


def lambda_handler(event, context):
    try:
        input_key = INPUT_KEY
        if isinstance(event, dict) and event.get("input_key"):
            input_key = event["input_key"]

        result = process_profiles(
            data_lake_bucket=DATA_LAKE_BUCKET,
            input_key=input_key,
            api_key=PDL_API_KEY,
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "message": "Profiles processed successfully",
                "output_key": result["output_key"],
                "record_count": result["record_count"],
            }),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": str(e),
            }),
        }