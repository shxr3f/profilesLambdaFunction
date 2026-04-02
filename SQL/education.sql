CREATE EXTERNAL TABLE profiles_bronze.education(
  person_id string, 
  education_index string, 
  school_name string, 
  school_type string, 
  school_id string, 
  school_location string, 
  school_linkedin_url string, 
  school_facebook_url string, 
  school_twitter_url string, 
  school_linkedin_id string, 
  school_website string, 
  school_domain string, 
  degrees string, 
  start_date string, 
  end_date string, 
  majors string, 
  minors string, 
  gpa string
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
  's3://profiles-dev-345895787413-ap-southeast-1/bronze/education/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);

MSCK REPAIR TABLE profiles_bronze.education;