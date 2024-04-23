# [wangrob]: should not need AWS credentials
# Desktop: Use "aws configure" to set up a profile.
# Lambda: Use Lambda execution environment default profile.
#aws_access_key_id = "<AWS_ACCESS_KEY_ID>"
#aws_secret_access_key = "<AWS_SECRET_ACCESS_KEY>"

# AWS parameters
profile_name = ""
region_name = ""

# api_script.py parameters
catapult_base_url = "<CATAPULT_BASE_URL>"
catapult_ftbl_id = "<CATAPULT_FTBL_ID>" #unused in API, but for reference
catapult_token = "<CATAPULT_TOKEN>"
catapult_username = "<CATAPULT_USERNAME>"
catapult_password = "<CATAPULT_PASSWORD>"
tags_csv_path = "2022_tags.csv"

# data_pulling_variables.py parameters
src_bucket_name = "<SRC_BUCKET_NAME>"
src_object_prefix = "<SRC_OBJECT_PREFIX>"

# data_transform.py parameters
final_data_path = "data_with_ac_3.csv"

# just_uploads.py parameters
dest_bucket_name = "<DEST_BUCKET_NAME>"
dest_object_prefix = "<DEST_OBJECT_PREFIX>"
files = ['activities22.csv',
    'recent_ac_data.csv',
    'full_ac_data.csv',
    'team_summary_2.csv',
    'position_summary.csv',
    '2022_tags.csv',
    'id_date_match.csv',
    'tagged_no_ac.csv']

# team_averages.py parameters
team_summary_csv_path = "team_summary_2.csv"
position_summary_csv_path = "position_summary.csv"

