from app.config import (
    DATA_LAKE_BUCKET,
    INPUT_PREFIX,
    RAW_PREFIX,
)
from app.handlers.input_handler import handle_input_csv
from app.handlers.raw_handler import handle_raw_json


def route_s3_event(bucket: str, key: str) -> dict:
    if bucket == DATA_LAKE_BUCKET and key.startswith(INPUT_PREFIX) and key.endswith(".csv"):
        return handle_input_csv(bucket=bucket, key=key)

    if bucket == DATA_LAKE_BUCKET and key.startswith(RAW_PREFIX) and key.endswith(".json"):
        return handle_raw_json(bucket=bucket, key=key)

    return {
        "status": "ignored",
        "bucket": bucket,
        "key": key,
        "reason": "no_matching_route",
    }