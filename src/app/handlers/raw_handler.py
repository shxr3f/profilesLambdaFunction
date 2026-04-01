from app.processors.bronze_processor import process_raw_to_bronze


def handle_raw_json(bucket: str, key: str) -> dict:
    result = process_raw_to_bronze(
        raw_bucket=bucket,
        raw_key=key,
    )

    return {
        "status": "processed",
        "stage": "bronze_transform",
        "raw_bucket": bucket,
        "raw_key": key,
        "people_output_key": result["people_output_key"],
        "experience_output_key": result.get("experience_output_key"),
        "education_output_key": result.get("education_output_key"),
        "profile_output_key": result.get("profile_output_key"),
        "person_count": result["person_count"],
        "run_id": result["run_id"],
    }