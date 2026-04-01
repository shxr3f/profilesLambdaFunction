import csv
import io
import json
import os
from pathlib import Path
import uuid
import pandas as pd

import boto3


s3 = boto3.client("s3")


def is_local_mode() -> bool:
    return os.getenv("LOCAL_MODE", "false").lower() == "true"


def read_csv(bucket: str, key: str) -> list[dict]:
    if is_local_mode():
        local_path = Path("local_input") / Path(key).name
        with open(local_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]

    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    return [dict(row) for row in reader]


def read_json(bucket: str, key: str) -> dict:
    if is_local_mode():
        local_path = Path("local_output") / Path(key).name
        with open(local_path, "r", encoding="utf-8-sig") as f:
            return json.load(f)

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8-sig")
    return json.loads(body)


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

def write_csv_rows(bucket: str, key: str, rows: list[dict]) -> None:
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

    if is_local_mode():
        local_path = Path("local_bronze") / key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "w", encoding="utf-8", newline="") as f:
            f.write(buffer.getvalue())
        return

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )