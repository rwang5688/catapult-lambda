import argparse
from datetime import date
from datetime import datetime
import json
import logging
import os
from pprint import pformat

import config


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
        
    config.src_bucket_name = get_env_var("SRC_BUCKET_NAME")
    if config.src_bucket_name == "":
        print("get_env_vars: failed to retrieve SRC_BUCKET_NAME.")
        return False
        
    config.dest_bucket_name = get_env_var("DEST_BUCKET_NAME")
    if config.dest_bucket_name == "":
        print("get_env_vars: failed to retrieve DEST_BUCKET_NAME.")
        return False
        
    # DEBUG
    print("get_env_vars:")
    print("region_name: %s" % (config.region_name))
    print("src_bucket_name: %s" % (config.src_bucket_name))    
    print("dest_bucket_name: %s" % (config.dest_bucket_name))
    
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
    
    # get environment variables
    if get_env_vars() == False:
        print("catapult-lambda: get_env_vars() failed.")
        return False
        
    # get event variables
    if get_event_vars(event) == False:
        print("catapult-lambda: get_event_vars() failed.")
        return False

    # set dest_object_prefix and dest_object_name
    today = date.today()
    print("catapult-lambda: today's date = %s" % (str(today)))
    
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d-%H%M%S")
    dest_object_name = "catapult-stats-"+timestamp+".csv"
    print("dest_object_name: %s" % (dest_object_name))
    
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
    
    