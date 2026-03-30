import os

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
DATA_LAKE_BUCKET = os.getenv("DATA_LAKE_BUCKET", "")
INPUT_KEY = os.getenv("INPUT_KEY", "raw/input/profiles.csv")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "raw/api_response/")
PDL_API_KEY = os.getenv("PDL_API_KEY", "")