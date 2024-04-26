import argparse
from datetime import date
from datetime import datetime
import json
import logging
import os
from pprint import pformat

import config
# [wangrob]: uncomment the following to start debugging
from update_existing_data_aws import update_existing_data_aws

LOGGER = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


def get_env_var(env_var_name):
    env_var = ""
    if env_var_name in os.environ:
        env_var = os.environ[env_var_name]
    else:
        print('get_env_var: Failed to get %s' % env_var_name)
    return env_var


def get_env_vars():
    config.region_name = get_env_var("REGION_NAME")
    if config.region_name == "":
        print("get_env_vars: failed to retrieve REGION_NAME.")
        return False
        
    config.catapult_base_url = get_env_var("CATAPULT_BASE_URL")
    if config.catapult_base_url == "":
        print("get_env_vars: failed to retrieve CATAPULT_BASE_URL.")
        return False
        
    config.catapult_token = get_env_var("CATAPULT_TOKEN")
    if config.catapult_token == "":
        print("get_env_vars: failed to retrieve CATAPULT_TOKEN.")
        return False
        
    config.catapult_ftbl_id = get_env_var("CATAPULT_FTBL_ID")
    if config.catapult_username == "":
        print("get_env_vars: failed to retrieve CATAPULT_FTBL_ID.")
        return False
        
    config.catapult_username = get_env_var("CATAPULT_USERNAME")
    if config.catapult_username == "":
        print("get_env_vars: failed to retrieve CATAPULT_USERNAME.")
        return False
        
    config.catapult_password = get_env_var("CATAPULT_PASSWORD")
    if config.catapult_password == "":
        print("get_env_vars: failed to retrieve CATAPULT_PASSWORD.")
        return False

    config.src_bucket_name = get_env_var("SRC_BUCKET_NAME")
    if config.src_bucket_name == "":
        print("get_env_vars: failed to retrieve SRC_BUCKET_NAME.")
        return False

    config.src_object_prefix = get_env_var("SRC_OBJECT_PREFIX")
    if config.src_object_prefix == "":
        print("get_env_vars: failed to retrieve SRC_OBJECT_PREFIX.")
        return False
        
    config.dest_bucket_name = get_env_var("DEST_BUCKET_NAME")
    if config.dest_bucket_name == "":
        print("get_env_vars: failed to retrieve DEST_BUCKET_NAME.")
        return False
        
    config.dest_object_prefix = get_env_var("DEST_OBJECT_PREFIX")
    if config.dest_object_prefix == "":
        print("get_env_vars: failed to retrieve DEST_OBJECT_PREFIX.")
        return False
        
    # DEBUG
    print("get_env_vars:")
    print("region_name: %s" % (config.region_name))
    print("catapult_base_url: %s" % (config.catapult_base_url))    
    print("catapult_token: %s" % (config.catapult_token))
    print("catapult_ftbl_id: %s" % (config.catapult_ftbl_id))
    print("catapult_username: %s" % (config.catapult_username))
    print("catapult_password: %s" % (config.catapult_password))
    print("src_bucket_name: %s" % (config.src_bucket_name))    
    print("src_object_prefix: %s" % (config.src_object_prefix))    
    print("dest_bucket_name: %s" % (config.dest_bucket_name))
    print("dest_object_prefix: %s" % (config.dest_object_prefix))
    
    return True
    
    
def get_event_vars(event):
    print("[DEBUG] get_event_vars: event = %s" % event)
    
    # DEBUG
    print("get_event_vars:")
    print("event: %s" % (event))
    
    return True
    
    
def lambda_handler(event, context):
    # start
    print('\nStarting lambda_function.lambda_handler ...')
    LOGGER.info("%s", pformat({"Context" : context, "Request": event}))
    if not os.path.exists('/tmp'):
        os.makedirs('/tmp')

    # get environment variables
    if get_env_vars() == False:
        print("catapult-lambda: get_env_vars() failed.")
        return False
        
    # get event variables
    if get_event_vars(event) == False:
        print("catapult-lambda: get_event_vars() failed.")
        return False

    # do something with date and time
    today = date.today()
    print("catapult-lambda: today's date is: %s" % (str(today)))
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    print("catapult-lambda: timestamp is: %s" % (timestamp))
    
    # execute update_existing_data_aws() function
    # [wangrob]: uncomment the following to start debugging
    update_existing_data_aws()
    
    # end
    print('\n... Thaaat\'s all, Folks!')
    
    
if __name__ == '__main__':
    # read arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--test-event", required=True, help="Test event.")
    args = vars(ap.parse_args())
    print("catapult-lambda: args = %s" % (args))
    
    # load json file
    test_event_file_name = args['test_event']
    f = open(test_event_file_name)
    event = json.load(f)
    f.close()
    print("catapult-lambda: test_event = %s" % (event))
    
    # create test context
    context = {}
    
    # Execute test
    lambda_handler(event, context)
    
    