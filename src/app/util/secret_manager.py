import json
import os
import boto3
from botocore.exceptions import ClientError

def get_secret(secret_arn: str, secret_key: str) -> str:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_arn)
    return response[secret_key]