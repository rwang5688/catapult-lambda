# %%
import api_script
import data_pulling_variables as variables
import pandas as pd 
import numpy as np
import data_transorming
from datetime import datetime 
import datetime as dt
import team_averages
import boto3
import time
import requests
import warnings
import just_uploads
warnings.filterwarnings("ignore")


# %%
def activitity_ids_2(after_date=1641026743, write=True):
    print(f"dates after {dt.date.fromtimestamp(after_date)}")
    #if after_date != None:
    activities = api_script.get_activities(write=True, after_date=after_date)
    
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

    df = pd.DataFrame(data= {"date_id" : dates, "ids" : ids})
    df = df[df["date_id"] > dt.datetime.fromtimestamp(after_date)]
    df['date_id'] = pd.to_datetime(df['date_id'])
    
    df.to_csv('/tmp/id_date_match_2.csv')

    df['tag'] = pd.Series(df['ids']).apply(lambda x :  md_tag_dict[x])
    #id_date_dict = dict(zip(df['ids'],df['date_id']))
    #print(df)
    #ids_df = pd.DataFrame(df[['date_id','ids']])
    #df = df.groupby("date_id")['tag'].apply(tag_consolidate).reset_index() 

    #Trying to deal with the tag consolidation and activity consolidation at once later on

    #df["ids"] = ids_df["ids"]
    #df.to_csv('/tmp/2022 tags_2.csv') #removing this for now as i dont think tags 2022_2 are needed to be read again, we can just save them and reuse variable 
    
    #pickle ids
    return df, ids


# %%
r = {}
#Metrics/stats for new activities
def get_new_stats(new_ids):
    #global r
    i = 0
    dfs = pd.DataFrame()

    len_ids = len(new_ids)
    print(len_ids)
    for practice_id in new_ids:
            i += 1
            if i%20 == 0:
                print(f"{(i/len_ids) * 100}% complete" )
            data = api_script.get_stats_activity(practice_id)
            dfs = pd.concat([dfs, data], axis=0)
    
    athlete_info_df = pd.DataFrame()#columns=["id","name","position_name",'tag_0','practice_tag'])

    for practice_id in new_ids:
        # we can also get more info on height, weight, age, HR...  
        #print("tag")
        url = f"{api_script.base_url}/activities/{practice_id}/athletes"
        r = requests.get(url, headers = api_script.header_arg,verify=False)
    
        #probably can use pd.read_json ...
        
        for athlete in r.json():
            try:
                id = athlete['id']
                name = athlete['first_name'] + " " + athlete['last_name']
                position_name = athlete['position_name']
                if position_name == 'DB': position_name = "SAF"    
                tag_list = athlete['tag_list']
                practice_tag = practice_id
                curr_df  = pd.DataFrame([{"id":id, "name":name,"position_name":position_name,"practice_tag": practice_tag}])
                #Can we add date for readability? - would have to save it from get_stats_activity before the for loop
                for i in range(len(tag_list)): curr_df[f"tag_{i}"] = tag_list[i]
                if tag_list == []:
                    curr_df['tag_0'] = 'Full'
                athlete_info_df = pd.concat([athlete_info_df, curr_df],axis=0)
            except TypeError:
                print("TypeError")
                print(athlete)
                print(type(athlete))
        #merged = pd.merge(athlete_info_df,id_date_match,left_on='practice_tag',right_on='ids')
        #merged.to_csv("/tmp/date_athlete_tag_merged.csv")
    
    dfs.to_csv("/tmp/activities22.csv")
    athlete_info_df.to_csv("/tmp/athlete_info.csv")
    return dfs,athlete_info_df


unknown_tags = []
def availabiliy_tag_processing(availability_tags):
        if type(availability_tags) == str:
            return availability_tags
        else:
            availability_tags = list(availability_tags) 
        if 'Rehab' in availability_tags:
            return 'Rehab'
        elif 'Modified' in availability_tags:
            return 'Modified'
        elif 'Full' in availability_tags:
            return 'Full'
        else:
            unknown_tags.append(availability_tags)
            return availability_tags[0]
            
            
#This function defines how we should deal with different metrics in the case that we have multiple practices in the same day 
#  some we will want to take the max or the min, most we just add together 
def create_agg_dict(df):
    #Takes in argument df, which has the columns we want to aggregate
    agg_dict = {}
    min_columns = ['max_effort_deceleration',]
    max_columns = [ 'max_vel','athlete_max_velocity','max_effort_acceleration', 'explosive_efforts_per_minute','accel_load_density_index', 'acceleration_density', 'hsd_yds_%','acceleration_load_average', 'player_load_per_minute','meterage_per_minute','percentage_max_velocity','gen2_acceleration_band2plus_average_effort_count']
    avg_columns = []
    sum_columns = set(df.columns) - set(min_columns) - set(max_columns) - set(['date_id','athlete_name','athlete_id'])

    unneeded = ['errors.accel_load_density_index.type','Unnamed: 0.1','errors.gen2_velocity_band8_average_effort_distance.type','errors.gen2_velocity_band8_average_effort_distance.message','practice_tag','errors.hsd_yds_%.message','errors.hsd_yds_%.type','start_time_h','end_time_h','date']#,'average_distance_session','velocity_band3_average_distance_session','average_duration_session']

    for i in df.columns:
        if 'error' in i or 'date' in i or 'Unnamed' in i or 'position' in i : 
            unneeded.append(i)
    sum_columns -= set(unneeded)
    sum_columns
    
    for col in df.columns:
        if col in min_columns:
            agg_dict[col] = 'min'
        elif col in max_columns:
            agg_dict[col] = 'max'
        #else:
        #    agg_dict[col] = 'sum'

    return agg_dict, sum_columns
    
    
# %%
def resample_data(updated_ac_data):
    return # This function is to add rows for each athlete for each day, creating new NA rows if there are missing days
    #This is not needed anymore, because we deal with missing days in the graphing functions as needed
    data_counts = updated_ac_data.groupby(['date_id','athlete_name','id']).count().sort_values('date_id',ascending=False)
    if data_counts.max().max() > 1:
        #Raise an error saying that there are duplicate activities for at least one athlete on a given day
        print("ERROR: There are duplicate activities for at least one athlete on a given day")
        print(data_counts[data_counts['total_player_load'] > 1])
        raise Exception("ERROR: There are duplicate activities for at least one athlete on a given day") 
        
    else:
        #resample test_ac_data so that each athlete has a row for each day
        updated_ac_data.reset_index(inplace=True)
        updated_ac_data.set_index(['date_id','athlete_name','id'],inplace=True)
        
        # group by athlete_name
        grouped = updated_ac_data.groupby(['athlete_name','id'])

        def resample_func(x):
            #get duplicte dates
            vc = x.index.value_counts()
            if len(vc[vc > 1]) > 0:
                print(len(vc[vc > 1]))
                x = x.resample('D').asfreq()#.reset_index()
                x.index = x.index.date
            return x
        # resample by day and fill in missing values with NaNs
        resampled = grouped.apply(resample_func)

        # reset index
        #drop only the id from the multilevel index
        #resampled.reset_index(level=['athlete_name','id'], drop=True, inplace=True)
        resampled.reset_index(inplace = True)
        resampled['date_id'] = resampled['date_id'].dt.date
        return resampled
        
        
# main function
def update_existing_data_aws():
    #just_uploads.download_csv_s3()
    
    # %%
    exisiting_activities = variables.s3_read('activities22.csv')
    exisiting_activities.set_index('date_id',inplace=True)
    exisiting_activities.index = pd.to_datetime(exisiting_activities.index)
    exisiting_activities.index.value_counts().sort_index().tail(10)
    
    # %%
    DAYS = 5
    pull_since = exisiting_activities.index.max() - dt.timedelta(days=DAYS)
    print(exisiting_activities.index.max() , dt.timedelta(days=DAYS))
    print(f"before pull since: {pull_since}")
    
    old_activities = exisiting_activities[exisiting_activities.index < pull_since]#All activities on or after pull_since
    print("Old Activities: number of activities, metrics = ",old_activities.shape)
    old_activities.head(5)
    
    # %%
    #origingal_id_date_match = pd.read_csv('/tmp/id_date_match.csv')
    #origingal_id_date_match['date_id'] = pd.to_datetime(origingal_id_date_match['date_id'])
    #origingal_id_date_match = origingal_id_date_match[['date_id', 'ids']]
    
    # %%
    new_ids_tags_df, ids = activitity_ids_2(after_date=datetime.timestamp(pull_since))
    #print(ids)
    new_ids_tags_df
    
    r = {} # [wangrob]: re-initialize global variable
    new_activities, new_athlete_info = get_new_stats(ids) 
    new_activities['date_id'] = pd.to_datetime(new_activities['date_id']).dt.date
    
    # %%
    datetime_id_dict =  dict(zip(new_ids_tags_df['ids'],new_ids_tags_df['date_id']))
    
    # %%
    new_athlete_info['date_id'] = new_athlete_info['practice_tag'].apply(lambda x : datetime_id_dict[x])
    new_athlete_info['date_id'] = pd.to_datetime(new_athlete_info['date_id'])
    new_athlete_info['date'] = new_athlete_info['date_id'].dt.date
    
    individual_tag_columns = [] #it might be more concise to get the tag list from the api call and break it into columns after this step
    for c in new_athlete_info.columns:
        if 'tag_' in c:
            individual_tag_columns.append(c)
    
    unknown_tags = [] # [wangrob]: re-initialize global variable
    daily_new_athlete_info = new_athlete_info.groupby(['id','date','name','position_name'])
    #select the tag_ columns and apply the availability_tag_processing function
    daily_new_athlete_info = daily_new_athlete_info[individual_tag_columns].agg(availabiliy_tag_processing).reset_index()

    print("unknown_tags: ", unknown_tags)
    #print("Found new/na tags in tag processing:",np.unique(unknown_tags))
    daily_new_athlete_info.rename(columns={'date':'date_id'},inplace=True)
    daily_new_athlete_info

    # %%
    tmp = new_activities.loc[:,['date_id','practice_tag','athlete_id','athlete_name','total_player_load','total_duration']]
    tmp = new_ids_tags_df.merge(tmp,left_on='ids',right_on='practice_tag')
    def get_mode(x):
        return x.value_counts().index[0]
    daily_new_ids_tags = tmp.groupby(['date_id_y'])[['tag','practice_tag']].agg(get_mode).reset_index()
    daily_new_ids_tags.rename(columns={'date_id_y':'date_id'},inplace=True)
    daily_new_ids_tags
    daily_new_athlete_info

    # %%
    new_athlete_info_and_tags = daily_new_athlete_info.merge(daily_new_ids_tags,on='date_id')
    #not_in_activities = set(new_athlete_info_and_tags.groupby(['id','date_id']).count().index) - set(new_activities.groupby(['athlete_id','date_id']).count().index)
    new_athlete_info_and_tags.to_csv('/tmp/new_athlete_info_and_tags.csv',index=False)
    #quit()
    # %%
    ##Group data by day, combining all activities for that day
    #Take the most common practice tag for Full players for that day, have that be the practice tag, and tag value we keep, making a 
    
    agg_dict, sum_columns = create_agg_dict(new_activities)
    grouped_max_min = new_activities.groupby(['date_id','athlete_name','athlete_id']).agg(agg_dict)
    
    sum_columns = list(sum_columns)
    for i in ['date_id','athlete_name', 'athlete_id']:
        sum_columns.append(i)
    
    grouped_sum = new_activities.loc[:,sum_columns].groupby(['date_id','athlete_name','athlete_id']).sum()
    merged_activities = pd.merge(grouped_max_min, grouped_sum,left_index=True,right_index=True)
    merged_activities = merged_activities.reset_index().sort_values('date_id',ascending=False)
    new_daily_activities = merged_activities.copy()
    
    # %%
    new_daily_activities = new_daily_activities[new_daily_activities['date_id'] >= pull_since]
    exisiting_activities = exisiting_activities[exisiting_activities.index < pull_since]
    exisiting_activities.reset_index(inplace=True)
    exisiting_activities['date_id'] = pd.to_datetime(exisiting_activities['date_id']).dt.date
    new_daily_activities['date_id'] = pd.to_datetime(new_daily_activities['date_id']).dt.date
    updated_activities = pd.concat([exisiting_activities,new_daily_activities],axis=0)
    updated_activities.reset_index(inplace=True,drop = True)
    updated_activities.to_csv('/tmp/activities22.csv',index = False)
    updated_activities['date_id'] = pd.to_datetime(updated_activities['date_id'])
    print(updated_activities['date_id'].max())
    updated_activities['date_id'].min()

    # %%
    
    
    # %%
    print(new_daily_activities.shape)
    new_tagged_no_ac = new_daily_activities.merge(new_athlete_info_and_tags,left_on=['date_id','athlete_name','athlete_id'],right_on=['date_id','name','id'])
    new_tagged_no_ac

    # %% [markdown]
    # Next: 
    # - Get all tagged past activities, with and without added AC metrics (AC metrics includes Zscores, ACWR, etc. everything that we add on to Catapult)
    # - Combine with new tagged activities to calculate the new AC stats 
    
    # %% [markdown]
    # The  tagged no ac csv for the past data is not aligned with  activities_22.csv - so we really need to redownload/recalculate tagged no ac for all the past data or at least make sure that while we are still developping this, we ensure that we do not miss data inbetween the most recent date in tagged_no_ac.csv and activities_22.csv
    
    # %%
    past_tagged_no_ac = variables.s3_read('tagged_no_ac.csv')
    past_tagged_no_ac['date_id'] = pd.to_datetime(past_tagged_no_ac['date_id']).dt.date
    past_tagged_no_ac.set_index('date_id',inplace=True)
    past_tagged_no_ac.sort_index(inplace=True)
    past_tagged_no_ac.index.value_counts().sort_index()
    print("max date in exisiting activities", exisiting_activities.index.max())
    print('max date in tagged_no_ac', past_tagged_no_ac.index.max())
    new_tagged_no_ac.set_index('date_id',inplace=True)
    new_tagged_no_ac.sort_index(inplace=True)
    print('min new tagged ac data date: ', new_tagged_no_ac.index.min())
    
    # %%
    past_tagged_no_ac = past_tagged_no_ac[past_tagged_no_ac.index < pull_since]
    print('past tagged no ac before pull since', past_tagged_no_ac.index.max())
    print('past tagged no ac shape', past_tagged_no_ac.shape)
    
    # %%
    new_tagged_no_ac.index.min()
    new_data_missing_columns = set(past_tagged_no_ac.columns) - set(new_tagged_no_ac.columns)
    print("Dropping missing columns in the new update: ", new_data_missing_columns)
    past_tagged_no_ac.drop(columns=new_data_missing_columns,inplace=True)
    old_data_missing_columns = set(new_tagged_no_ac.columns) - set(past_tagged_no_ac.columns)
    print('Dropping new columns not contained in old data:' ,old_data_missing_columns)
    print('If we want to keep these columns, we have to rerun the script on the old data to get the data for all past activities')
    new_tagged_no_ac.drop(columns=old_data_missing_columns,inplace=True)
    
    # %%
    updated_tagged_no_ac = pd.concat([past_tagged_no_ac,new_tagged_no_ac],axis=0)
    updated_tagged_no_ac.to_csv('/tmp/tagged_no_ac.csv')
    #We have to get the last 30 days before the oldest activity in the update to calculate rolling metrics
    recent_tagged_no_ac = updated_tagged_no_ac[updated_tagged_no_ac.index > pull_since -dt.timedelta(days=33)] 
    recent_tagged_no_ac.reset_index(inplace=True)
    recent_tagged_no_ac['date_id'] = recent_tagged_no_ac['date_id'].astype(str)
    
    recent_ac_data_no_tag = data_transorming.add_rolling_metrics(recent_tagged_no_ac) #We lose tag_0 and tag_1 here - either need to remerge data, or adjust how the function does groupby
     
    # %%
    #we have to add tag_0, tag_1 and tag_2 back in if they existed before calculating rolling metrics,
    # because currently the groupby operation when calculating rolling metrics drops them
    tag_cols = []
    for c in new_athlete_info_and_tags.columns:
        if 'tag_' in c:
            tag_cols.append(c)
    needed_availability_tags_cols = ['date_id','name','id',] + tag_cols 
    #tmp = new_athlete_info_and_tags.loc[:,needed_availability_tags_cols].rename(columns = {'name':'athlete_na'})
    new_athlete_info_and_tags['date_id'] = pd.to_datetime(new_athlete_info_and_tags['date_id'])
    
    new_ac_data = recent_ac_data_no_tag.merge(new_athlete_info_and_tags,left_on=['date_id','athlete_name'],right_on=['date_id','name'],suffixes=('','_drop'))
    
    to_drop = []
    for c in new_ac_data.columns:
        if '_drop' in c:
            to_drop.append(c)
    new_ac_data.drop(columns=to_drop,inplace=True)
    
    # %%
    
    # %%
    exisiting_ac_data = variables.s3_read('full ac data.csv')
    exisiting_ac_data['date_id'] = pd.to_datetime(exisiting_ac_data['date_id']).dt.date
    exisiting_ac_data = exisiting_ac_data[exisiting_ac_data['date_id'] < pull_since]
    #exisiting_ac_data.loc[:,['date_id','tag_0']]
    #exisiting_ac_data['tag'].value_counts()

    # %%
    print(new_ac_data['date_id'].min())
    updated_ac_data = pd.concat([exisiting_ac_data,new_ac_data],axis=0)
    print(new_ac_data.shape)
    print(exisiting_ac_data.shape)
    print(new_ac_data.shape[0] + exisiting_ac_data.shape[0])
    updated_ac_data
    to_drop = []
    for i in updated_ac_data.columns:
        if 'Unnamed' in i:
            print('Dropping Column: ', i)
            to_drop.append(i)
    updated_ac_data.drop(columns=to_drop,inplace=True)
    updated_ac_data['tag']

    
    # %%
    recent_ac_data = updated_ac_data[updated_ac_data['date_id'] > pull_since - dt.timedelta(days=30*9)]
    
    # %%
    #just_uploads.download_csv_s3(['team_summary_2.csv','position_summary.csv'])
    exisiting_team_summary = variables.s3_read('team_summary_2.csv')
    exisiting_position_summary = variables.s3_read('position_summary.csv')
    
    exisiting_team_summary['date_id'] = pd.to_datetime(exisiting_team_summary['date_id']).dt.date
    exisiting_position_summary['date_id'] = pd.to_datetime(exisiting_position_summary['date_id']).dt.date
    exisiting_team_summary = exisiting_team_summary[exisiting_team_summary['date_id'] < pull_since]
    exisiting_position_summary = exisiting_position_summary[exisiting_position_summary['date_id'] < pull_since]
    exisiting_team_summary 
    exisiting_position_summary
    
    
    # %%
    exisiting_position_summary['date_id'].min()
    
    # %%
    recent_ac_data['date_id'] = pd.to_datetime(recent_ac_data['date_id']).dt.date
    recent_ac_data['date_id'].min()
    recent_ac_data['date_id'].max()
    
    # %%
    recent_ac_data['date_id'] = recent_ac_data['date_id'].astype(str)
    recent_ac_data['tag'] = recent_ac_data['tag'].astype(str)
    
    recent_team_summary, recent_position_summary = team_averages.team_averages(recent_ac_data,to_csv=False)
    recent_position_summary.reset_index(inplace=True)
    recent_team_summary.reset_index(inplace=True)
    
    # %%
    recent_team_summary['date_id'] = pd.to_datetime(recent_team_summary['date_id']).dt.date
    recent_position_summary['date_id'] = pd.to_datetime(recent_position_summary['date_id']).dt.date
    recent_position_summary
    new_team_summary, new_position_summary = recent_team_summary[recent_team_summary['date_id'] >= pull_since], recent_position_summary[recent_position_summary['date_id'] >= pull_since]
    updated_team_summary, updated_position_summary = pd.concat([exisiting_team_summary,new_team_summary],axis=0), pd.concat([exisiting_position_summary,new_position_summary],axis=0)
    updated_team_summary
    
    # %%
    #updated_position_summary.to_csv('/tmp/position_summary.csv')
    #updated_team_summary.to_csv('/tmp/team_summary_2.csv')
    print(updated_team_summary['date_id'].min(),updated_position_summary['date_id'].max()
    )#get columns containing player_load
    player_load_cols = ['date_id']
    for c in updated_ac_data.columns:
        if 'player_load' in c:
            player_load_cols.append(c)
    updated_ac_data.loc[:,player_load_cols]
    
    # %%
    updated_team_summary.to_csv('/tmp/team_summary_2.csv',index = False)
    updated_position_summary.to_csv('/tmp/position_summary.csv',index = False)
    updated_ac_data.to_csv('/tmp/full_ac_data.csv',index = False)
    recent_ac_data = recent_ac_data[pd.to_datetime(recent_ac_data['date_id']) > pd.to_datetime(pull_since) - dt.timedelta(days=90)]
    recent_ac_data.reset_index(inplace=True)
    recent_ac_data.to_csv('/tmp/recent_ac_data.csv',index = False)
    
    # %%
    just_uploads.upload_csvs_s3()
    
    