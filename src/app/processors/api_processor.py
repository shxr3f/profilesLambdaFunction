from datetime import datetime, timezone
import uuid

from app.config import RAW_PREFIX
from app.util.pdl_client import enrich_person, build_pdl_client
from app.util.s3_io import read_csv, write_json

def process_profiles(data_lake_bucket: str, input_key: str, api_key: str) -> dict:
    records = read_csv(data_lake_bucket, input_key)

    client = build_pdl_client(api_key)

    output = []
    for idx, record in enumerate(records):
        first_name = record.get("first_name")
        last_name = record.get("last_name")

        result = enrich_person(first_name=first_name, last_name=last_name, client=client)

        data = result["body"].get("data", [])

        output.extend(data)

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    date_partition = now.strftime("%Y-%m-%d")
    run_id = uuid.uuid4().hex[:8]

    output_key = (
        f"{RAW_PREFIX}date={date_partition}/"
        f"{timestamp}_{run_id}.json"
    )

    payload = {
        "run_timestamp_utc": timestamp,
        "run_id": run_id,
        "record_count": len(records),
        "source_bucket": data_lake_bucket,
        "source_key": input_key,
        "output_key": output_key,
        "results": output,
    }

    write_json(data_lake_bucket, output_key, payload)

    return payload