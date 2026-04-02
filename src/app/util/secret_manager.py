import json
import boto3
import os
from botocore.exceptions import ClientError

def is_local_mode() -> bool:
    return os.getenv("LOCAL_MODE", "false").lower() == "true"

def get_secret(secret_arn: str, secret_key: str) -> str:
    if is_local_mode():
        return os.getenv("PDL_API_KEY", "key")

    client = boto3.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_arn)

    secret_string = response.get("SecretString")
    if not secret_string:
        raise ValueError("SecretString is empty")

    secret_dict = json.loads(secret_string)

    return secret_dict[secret_key]