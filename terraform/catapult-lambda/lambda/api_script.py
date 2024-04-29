from operator import ilshift
import re
import requests
import pandas as pd
from datetime import datetime
import json
import os
#import plotly.express as px
import datetime as dt
import data_pulling_variables as variables
import boto3
import pickle

import config

base_url = config.catapult_base_url
token = config.catapult_token
ftbl_id = config.catapult_ftbl_id #unused in API, but for reference
username = config.catapult_username
password = config.catapult_password
header_arg = {"Authorization" : f"Bearer {token}"}
header_arg_stats = {"Authorization" : f"Bearer {token}",'content-type': "application/json"}
header_arg2 = {'content-type': "application/json"}
tags_csv_path = config.tags_csv_path

# Stuff for authentication token, which we already have
auth_data  = {
    "client_id": token,
    "client_secret": token,
    "username": username,
    "password": password,
    "grant_type": "password"
}
#r = requests.post(f"{base_url}/oauth/token")#, headers= { 'content-type': 'application/json'}, data= data )
#print(r.json())

'''
for i in range(len(desired_params)):
    desired_params[i] = desired_params[i].lower().replace(" ", "_")
print(desired_params)
'''
desired_slugs = variables.needed_vars
# TO improve load time - serialize whether or not we need to update, 


def get_teams(read_write = "w", as_list = False):
    teams = requests.get(f"{base_url}/teams", headers = header_arg,verify = False) 
    if as_list:
        teams_json = teams.json()
        teams_list = []
        for i in teams_json:
            teams_list.append(i["name"])
        return teams_list

    return teams.json()
    
    
def get_activities(write = False, after_date = None):
    #no support for before
    # No support for date dependent calls (calling write = False will not have most up to date stats)
    # Best way to support multidate calls is to name csvs/jsons based on the call date, to see if we have it
    # probably should seperate to write and get functions, with global variables for fp
    url = f"{base_url}/activities?embed=all"
    if after_date != None:
        url = f"{base_url}/activities?startTime={after_date}" #idk how to get embed all and date - need to use Postman if this needs to be figured out in the future 

    if write:
        
        r = requests.get(url, headers = header_arg,verify = False)
        # 'current_team_id' param - name: calberkeleyfootball
        activities_js = r.json()
        activities_dump = json.dumps(activities_js,indent = 4)
        try:
            with open("full_activities.json", 'w', encoding="utf-8") as outfile: outfile.write(activities_dump)
        except:
            with open("full_activities.json", 'x', encoding="utf-8") as outfile: outfile.write(activities_dump)
    
        return activities_js

    else:
        with open("full_activities.json", 'r', encoding="utf-8") as f:
            activities_js = json.loads(f.read())
        return activities_js


def get_stats_activity(id, group_by = None, params = variables.needed_vars):
    data_arg = '''{
    "filters": [
    {
    "name": "activity_id",
    "comparison": "=",
    "values": ["''' + f"{id}"+'''"]
    }
    ],
    "parameters": [
    "explosive_efforts_per_minute",
    "gen2_acceleration_band2plus_average_effort_count",
    "explosive_efforts",
    "average_distance",
    "accel_load_density_index",
    "acceleration_density",
    "hsd_yds_%",
    "gen2_acceleration_band7plus_average_effort_count",
    "high_speed_running",
    "gen2_velocity_band8_average_effort_distance",
    "gen2_velocity2_band8_total_effort_count",
    "total_distance_(y)_5mph<",
    "gen2_acceleration_band7plus_total_effort_count",
    "max_effort_deceleration",
    "gen2_acceleration_band2plus_total_effort_count",
    "total_acceleration_load",
    "acceleration_load_average",
    "hsd_yds_70%_<",
    "athlete_max_velocity",
    "gen2_acceleration_band2plus_average_effort_count",
    "sprint_count_(b7+)_gen_2",
    "average_player_load_session",
    "max_effort_acceleration",
    "player_load_per_minute",
    "total_player_load",
    "percentage_max_velocity",
    "velocity2_band8_total_distance",
    "max_vel",
    "total_duration",
    "high_speed_running_(avg)",
    "average_distance_session",
    "gen2_acceleration_band7plus_total_distance",
    "gen2_acceleration_band2plus_total_distance",
    "total_distance",
    "average_duration_session",
    "gen2_velocity_band8_total_effort_count",
    "gen2_acceleration_band2plus_average_effort_count",
    "velocity2_band8_average_distance",
    "meterage_per_minute"
    ],
    "group_by": [
    "athlete"
    ]
    }'''    #print(data_arg)
    for v in variables.needed_vars:
        if not v in data_arg:
            raise Exception(f"Need to add {v} to metrics requested from the api")
    x  = requests.post(f"{base_url}/stats", headers = header_arg_stats, data= data_arg,verify = False)
    data =  pd.json_normalize(x.json())
    data['practice_tag'] = id
    return data
    
    
def activitity_ids(after_date = 1641026743, write = True):
    if write == False:
        print("FIX THIS")
        #read and return pickled ids
        return pd.read_csv(tags_csv_path).to_dict() 
    print(after_date)
    print(f"dates after {dt.date.fromtimestamp(after_date)}")
    #if after_date != None:
    activities = get_activities(write = True, after_date= after_date)
    #else: #This makes no sense to me
    #    activities = get_activities()
    ids = []
    md_tag_dict = {}
    dates = []
    i = 0
    for activity in activities:

        #if i == 66: print("activity keys", activity.keys())
        found_tag = False
        for tag in (activity["tag_list"]):
            #(tag)
            if tag['tag_type_name'] == "Activity": #This is the "Activity" tag. Alternatively could base on "tag_type_name" == "Activity" 
                #print(tag['tag_name'])
                x = (tag["tag_name"]) #there are multiple Activity tag values
                print('tag_name', x)
                #if "MD" in x and not " " in x:
                md_tag_dict[activity["id"]] = x
                found_tag = True
        #print(activity["athlete_count"])
        if found_tag == False:
            md_tag_dict[activity["id"]] = "na"
        i += 1
        ids.append(activity["id"])
        dates.append(dt.datetime.fromtimestamp(activity["start_time"]))
        
        #print(activity["tags"])

    filt_dates = []
    for date in dates:
        if date >= dt.datetime(2023,1,1):
            filt_dates.append(date)
    
    
    def tag_consolidate(lst):

        lst = list(lst)
        print('to consolidate:',lst)
        if len(lst) == 1:
            return lst[0]
        
        for i in lst:
            if "MD" in i:
                return i
        return 'na'


    print(len(ids))
    print(len(md_tag_dict))
    print(len(dates),len(pd.Series(dates).unique()))
    print(len(filt_dates),len(pd.Series(filt_dates).unique()))
    print(f"min date: {pd.Series(dates).min()}")

    
    df = pd.DataFrame(data= {"date_id" : dates, "ids" : ids})
    df = df[df["date_id"] > dt.datetime.fromtimestamp(after_date)]
    df['date_id'] = pd.to_datetime(df['date_id'])
    df.to_csv('temp/id_date_match.csv')
    df['tag'] = pd.Series(df['ids']).apply(lambda x :  md_tag_dict[x])
    #df['ids'] = ids
    df = df.groupby("date_id")['tag'].apply(tag_consolidate)
    df.to_csv(tags_csv_path)
    print(df)
    #pickle ids
    return None, ids
#activitity_ids(after_date=1641026743)


def get_params():
    params = requests.get(f"{base_url}/parameters", headers = header_arg,verify = False)
    pjs = params.json()
    param_names = [] 
    param_slugs = []   
    des_slugs = []
    des_names = []
    for p in pjs:
        name = p["name"]
        slug = p['slug']
        param_names.append(name)
        param_slugs.append(slug)
        if name in variables.needed_vars:
            des_slugs.append(slug)
            des_names.append(name)


    try:
        with open("params.txt", 'w', encoding="utf-8") as f:
            f.write(str(param_slugs))
    except:
        with open("params.txt", 'x', encoding="utf-8") as f:
            f.write(str(param_slugs))

    try:
        with open("param_names.txt", 'w', encoding="utf-8") as f:
            f.write(str(param_names))
    except:
        with open("params_names.txt", 'x', encoding="utf-8") as f:
            f.write(str(param_names))


    print((pjs[0]))
    return des_names, des_slugs, dict(zip(param_names,param_slugs)),dict(zip(param_slugs,param_names))


def update_dicts():
    nts,stn =get_params()[2:4]
    #txt_nts = pickle.dump(nts)
    try:
        with open("nts_dict.txt", 'w', encoding="utf-8") as outfile:  outfile.write(json.dumps(nts))
    except:
        with open("nts_dict.txt", 'x', encoding="utf-8") as outfile: outfile.write(json.dumps(nts)) #outfile.write(txt_nts) 

    #txt_stn = pickle.dump(stn)
    try:
        with open("stn_dict.txt", 'w', encoding="utf-8") as outfile:  outfile.write(json.dumps(stn))#outfile.write(txt_stn)
    except:
        with open("stn_dict.txt", 'x', encoding="utf-8") as outfile: outfile.write(json.dumps(stn))#outfile.write(txt_stn) 
#update_dicts()


def find_param(search):
    params = requests.get(f"{base_url}/parameters", headers = header_arg,verify = False)
    pjs = params.json()
    slug_matches = []
    for p in pjs:
        name = p["name"]
        slug = p['slug']
        print(search)
        if search in slug:
            slug_matches.append(slug) 
    return slug_matches


def all_activities_stats(write = False, unix_start = datetime(2023,1,1).timestamp()): 
    if write:
        #print("starting write")
        dfs = pd.DataFrame()
        activity_tags, all_ids = activitity_ids(after_date= unix_start)
        len_ids = len(all_ids)
        print(f"Unix start: {unix_start}, gathering {len_ids} practices")
        i = 0
        for id in all_ids:
            i += 1
            if i%20 == 0:
                print(f"{(i/len_ids) * 100}% complete" )
            data = get_stats_activity(id)
            dfs = dfs.append(data)

        #Let's add rolling stats here - need to recalculate last 28 days upon start, for now can just recalculate for all of them 
        # Add A:C ratios, should just need to divide columns


        # - that is the current workflow to get data should be singular call to this function, from which rolling stats are calculate, and then A:C ratios are added
        # ^^ this will need to be done and run in entirety to update data after each new day. Afterwards this can be sped up by only adding the next days activities, and then recalculating
        
        # From that we can just recalculate for the past 29 days 
        

        dfs.to_csv("temp/activities22.csv")
        #need to set up config file to use boto 
        #s3 = boto3.resource('s3')
        #s3.meta.client.upload_file('activities22.csv','cal-football-data','activities22.csv')
        return dfs
    else:
        pd.read_csv("temp/test activities22.csv")
#all_activities_stats(write = True, update=True)

#s3 = boto3.resource('s3')
#s3.meta.client.upload_file('activities22.csv','cal-football-data','activities22.csv')


def get_tags():
    #this is just to see what tags we have
    url = f"{base_url}/tags"
    r = requests.get(url, headers = header_arg,verify = False)
    print(r.json()[10])
    

def get_specific_activity_practice_tags(id):
    url = f"{base_url}/activities/{id}/athletes"
    r = requests.get(url, headers = header_arg,verify = False)

    return r.json()
    
    
def get_athlete_tags(write:bool,after_timestamp=datetime(2023,1,1).timestamp()):
    
    prev =  variables.s3_read('athlete_info.csv')
    
    if write == False:
        return prev
    #go through each practice id, 
    ids = activitity_ids(after_date=after_timestamp)[1]
    athlete_info_df = pd.DataFrame()#columns=["id","name","position_name",'tag_0','practice_tag'])

    for tag in ids:
        # we can also get more info on height, weight, age, HR...  
        #print("tag")
        url = f"{base_url}/activities/{tag}/athletes"
        r = requests.get(url, headers = header_arg,verify = False)
        #probably can use pd.read_json ...
        
        for athlete in r.json():
            try:
                id = athlete['id']
                name = athlete['first_name'] + " " + athlete['last_name']
                position_name = athlete['position_name']
                if position_name == 'DB': position_name = "SAF"    
                tag_list = athlete['tag_list']
                practice_tag = tag
                curr_df  = pd.DataFrame([{"id":id, "name":name,"position_name":position_name,"practice_tag": practice_tag}])
                #Can we add date for readability?
                for i in range(len(tag_list)): curr_df[f"tag_{i}"] = tag_list[i]
                athlete_info_df = pd.concat([athlete_info_df, curr_df],axis=0)
            except TypeError:
                print("TypeError")
                print(athlete)
                print(type(athlete))

    merged = pd.merge(pd.read_csv("temp/athlete_info.csv"),pd.read_csv('temp/id_date_match.csv'),left_on='practice_tag',right_on='ids')
    merged.to_csv("temp/date_athlete_tag_merged.csv")
    data = pd.read_csv(variables.data_path)
    print()
    
    athlete_info_df.to_csv('temp/athlete_info.csv',index=False)
    return athlete_info_df
#get_athlete_tags()


def merge_info_date_data(raw_data = 'activities22.csv', out_path = "tagged_no_ac_data.csv" ):
    #Function to be called after get_athlete_tags, and after data is gathered! Ideally move this to be called upon 

    #pd.merge(pd.read_csv("temp/athlete_info.csv"),pd.read_csv('temp/id_date_match.csv'),left_on='practice_tag',right_on='ids').to_csv("temp/date_athlete_tag_merged.csv")
    merged = pd.merge(pd.read_csv("temp/athlete_info.csv"),pd.read_csv('temp/id_date_match.csv'),left_on='practice_tag',right_on='ids')
    merged.rename(columns={"name":"athlete_name","position_name_x":"position_name"},inplace=True)
    merged['date_id'] = pd.to_datetime(merged['date_id'])
    tags = pd.read_csv(tags_csv_path)
    tags['date_id'] = pd.to_datetime(tags['date_id'])
    merged = merged.merge(tags,on='date_id')
    merged.to_csv("temp/date_athlete_tag_merged.csv")
    
    data = pd.read_csv(raw_data) #(variables.data_path)
    data['date_id'] = pd.to_datetime(data['date_id'])
     
    update_data = data.merge(merged, on=['practice_tag','athlete_name'],how='left',suffixes=('', '_remove'))
    print(update_data['tag'].replace("NaN",0)[update_data['tag']!=0])
    #update_data.drop([i for i in merged.columns if 'remove' in i], axis=1, inplace=True)
    #print("update_data",len(update_data), "data", len(data))
    update_data.to_csv(out_path,index=True)
    #print(update_data['tag_0'])
    #print(pd.read_csv)
    print("ENDDDDDDDDDDDDDDDDDD \n\n\n\n")
    print(update_data['tag'].unique())
    return update_data
    # There are some days with two practices!!
    
    
def activitity_ids_2(after_date = 1641026743, write = True):
    if write == False:
        print("FIX THIS")
        #read and return pickled ids
        return pd.read_csv(tags_csv_path).to_dict() 
    print(after_date)
    print(f"dates after {dt.date.fromtimestamp(after_date)}")
    #if after_date != None:
    activities = get_activities(write = True, after_date= after_date)
    #else: #This makes no sense to me
    #    activities = get_activities()
    ids = []
    md_tag_dict = {}
    dates = []
    i = 0
    for activity in activities:

        #if i == 66: print("activity keys", activity.keys())
        found_tag = False
        for tag in (activity["tag_list"]):
            #(tag)
            if tag['tag_type_name'] == "Activity": #This is the "Activity" tag. Alternatively could base on "tag_type_name" == "Activity" 
                #print(tag['tag_name'])
                x = (tag["tag_name"]) #there are multiple Activity tag values
                print('tag_names', x)
                #if "MD" in x and not " " in x:
                md_tag_dict[activity["id"]] = x
                    
                if x == 'NFL Group':
                    md_tag_dict[activity["id"]] = x
                #md_tag_dict[activity["id"]] = x
                found_tag = True
        #print(activity["athlete_count"])
        if found_tag == False:
            md_tag_dict[activity["id"]] = "na"
        i += 1
        ids.append(activity["id"])
        dates.append(dt.datetime.fromtimestamp(activity["start_time"]))
        
        #print(activity["tags"])

    filt_dates = []
    for date in dates:
        if date >= dt.datetime(2023,1,1):
            filt_dates.append(date)
    
    
    def tag_consolidate(lst):
        lst = list(lst)
        
        if len(lst) == 1:
            return lst[0]
        
        for i in lst:
            if "MD" in i:
                return i
        return 'na'

    '''
    print(len(ids))
    print(len(md_tag_dict))
    print(len(dates),len(pd.Series(dates).unique()))
    print(len(filt_dates),len(pd.Series(filt_dates).unique()))
    print(f"min date: {pd.Series(dates).min()}")'''

    #print('testing print')
    df = pd.DataFrame(data= {"date_id" : dates, "ids" : ids})
    df = df[df["date_id"] > dt.datetime.fromtimestamp(after_date)]
    df['date_id'] = pd.to_datetime(df['date_id'])
    df.to_csv('temp/id_date_match_2.csv')
    df['tag'] = pd.Series(df['ids']).apply(lambda x :  md_tag_dict[x])
    #df['ids'] = ids

    df = df.groupby("date_id")['tag'].apply(tag_consolidate)
    df.to_csv('temp/2022 tags_2.csv')
    
    return [df, ids]

'''
data = pd.read_csv("temp/test_big_merge_2.csv")
x = (data[(data["tag_0"]== "Starter")])
y = (data[data['tag_1'] == 'Full'])
print(len(x),len(y))
print(y[y['tag_0'] == 'Missing Data'])
print(y)
print(x)
print(data[data['tag_0']=="Full"])
print(data['tag_0'].unique())
'''

