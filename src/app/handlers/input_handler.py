from app.config import PDL_SECRET_ARN
from app.util.secret_manager import get_secret
from app.processors.api_processor import process_profiles


def handle_input_csv(bucket: str, key: str) -> dict:
    api_key = get_secret(secret_arn=PDL_SECRET_ARN, secret_key='PDL_API_KEY')
    result = process_profiles(
        data_lake_bucket=bucket,
        input_key=key,
        api_key=api_key,
    )

    return {
        "status": "processed",
        "stage": "raw_ingest",
        "input_bucket": bucket,
        "input_key": key,
        "output_key": result["output_key"],
        "record_count": result["record_count"],
        "run_id": result["run_id"],
    }