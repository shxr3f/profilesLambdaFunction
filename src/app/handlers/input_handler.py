from app.config import PDL_API_KEY
from app.processors.api_processor import process_profiles


def handle_input_csv(bucket: str, key: str) -> dict:
    result = process_profiles(
        data_lake_bucket=bucket,
        input_key=key,
        api_key=PDL_API_KEY,
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