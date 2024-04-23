import boto3
import pandas as pd

import config

dest_bucket_name = config.dest_bucket_name
dest_object_prefix = config.dest_object_prefix
files = config.files

def get_s3_client():
    # [wangrob]: should not need AWS credentials
    # Desktop: Use "aws configure" to set up a profile.
    # Lambda: Use Lambda execution environment default profile.
    #aws_access_key_id = config.aws_access_key_id
    #aws_secret_access_key = config.aws_secret_access_key
    #s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    print("get_s3_client: profile_name=%s, region_name=%s" % (config.profile_name, config.region_name))
    
    p_name = None
    if (config.profile_name != ''):
        p_name = config.profile_name
    session = boto3.Session(profile_name=p_name)
    s3 = session.client("s3", region_name=config.region_name)
    return s3
    
    
def upload_csvs_s3(file_list = files):
    s3 = get_s3_client()
    if s3 is None:
        print('upload_csvs_s3: Failed to get s3 client.')
        return None
        
    for file in files:
        s3.upload_file(file, dest_bucket_name, dest_object_prefix+file)
        print(file)
        
        
def download_csv_s3(file_list = files, multiple = False):
    s3 = get_s3_client()
    if s3 is None:
        print('upload_csvs_s3: Failed to get s3 client.')
        return None
        
    for file in file_list:
        #response = s3.get_object(Bucket='cal-football-data', Key=file)
        response = s3.get_object(Bucket=dest_bucket_name, Key=dest_object_prefix+file)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        if status == 200: 
            data = pd.read_csv(response.get("Body"))
            data.to_csv(file, index=False)
            print(file)
            if multiple == False:
                return data
        else:
            print(f"Something went wrong with {file}")
            print(f"Status code: {status}")
            return None
            
            
def clean_current_s3(file_list = files):
    download_csv_s3(file_list)
    for file in file_list:
        print('cleaning: ',file)
        data = pd.read_csv(file)
        for col in data.columns:
            if "Unnamed" in col:
                data.drop(col, axis=1, inplace=True)
            
        data.to_csv(file, index=False)
        print('cleaned: ',file)
        print(data.columns)
    upload_csvs_s3(file_list)
    
# do not uncomment and leave uncommented!!!
#upload_csvs_s3()
#download_csv_s3(multiple=True)
#upload_csvs_s3()
