CREATE EXTERNAL TABLE profiles_bronze.profiles(
  person_id string, 
  profile_index string, 
  network string, 
  profile_id string, 
  url string, 
  username string
)
PARTITIONED BY ( 
  date string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '"'
)
LOCATION
  's3://profiles-dev-345895787413-ap-southeast-1/bronze/profiles/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);

MSCK REPAIR TABLE profiles_bronze.profiles;