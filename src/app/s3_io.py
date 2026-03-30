import csv
import io
import json
import os
from pathlib import Path

import boto3


s3 = boto3.client("s3")


def is_local_mode() -> bool:
    return os.getenv("LOCAL_MODE", "false").lower() == "true"


def read_csv(bucket: str, key: str) -> list[dict]:
    if is_local_mode():
        local_path = Path("local_input") / Path(key).name
        with open(local_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]

    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return [dict(row) for row in reader]


def write_json(bucket: str, key: str, payload: dict) -> None:
    if is_local_mode():
        local_file = Path("local_output") / Path(key).name
        local_file.parent.mkdir(parents=True, exist_ok=True)

        with open(local_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )