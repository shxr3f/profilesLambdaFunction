import json
import boto3
from botocore.exceptions import ClientError

def get_secret(secret_arn: str, secret_key: str) -> str:
    client = boto3.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_arn)

    secret_string = response.get("SecretString")
    if not secret_string:
        raise ValueError("SecretString is empty")

    secret_dict = json.loads(secret_string)

    return secret_dict[secret_key]