import os

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "")
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "raw/input/")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/api_response/")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX", "bronze/")
PDL_SECRET_ARN = os.getenv("PDL_SECRET_ARN", "")