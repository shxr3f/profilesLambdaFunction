CREATE EXTERNAL TABLE profiles_bronze.experience(
  person_id string, 
  experience_index string, 
  company_name string, 
  company_size string, 
  company_id string, 
  company_founded string, 
  company_industry string, 
  company_industry_v2 string, 
  company_location string, 
  company_linkedin_url string, 
  company_linkedin_id string, 
  company_facebook_url string, 
  company_twitter_url string, 
  company_website string, 
  location_names string, 
  end_date string, 
  start_date string, 
  title_name string, 
  title_class string, 
  title_role string, 
  title_sub_role string, 
  title_levels string, 
  is_primary string
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
  's3://profiles-dev-345895787413-ap-southeast-1/bronze/experience/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
);

MSCK REPAIR TABLE profiles_bronze.experience;