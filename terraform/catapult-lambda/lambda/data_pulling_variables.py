import pandas as pd
import datetime as dt
import calendar
import numpy as np
import boto3
import pickle
import json

import config

src_bucket_name = config.src_bucket_name
src_object_prefix = config.src_object_prefix
tags_csv_path = config.tags_csv_path

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

    
def s3_read(file,return_object = False):
    #return pd.read_csv(file)
    
    s3 = get_s3_client()
    if s3 is None:
        return Exception("s3_read: Failed to get s3 client.")
    
    #response = s3.get_object(Bucket='cal-football-data', Key=file)
    response = s3.get_object(Bucket=src_bucket_name, Key=src_object_prefix+file)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200: 
        data = pd.read_csv(response.get("Body"))
        data.to_csv("tmp/"+file, index=False)
        print(file)
        return data
    else: 
        print(status)
        return Exception(status  + " Error with S3 reading")


with open("stn_dict.txt", 'r', encoding="utf-8") as f:
    stn = json.load(f)

with open("nts_dict.txt", 'r', encoding="utf-8") as f:
    nts = json.load(f)


summary_metrics = ['gen2_velocity2_band8_total_effort_count','accel_load_density_index','gen2_acceleration_band2plus_average_effort_count','gen2_acceleration_band7plus_average_effort_count','acceleration_load_average','total_distance_(y)_5mph<']
summary_metrics_readable = [stn[i] for i in summary_metrics].copy()

intensity_metrics = ['acceleration_load_average','hsd_yds_%','hsd_yds_70%_<','velocity2_band8_total_distance','gen2_acceleration_band7plus_total_distance','gen2_acceleration_band2plus_total_distance'] 
density_metrics = ['accel_load_density_index','player_load_per_minute','meterage_per_minute']
volume_metrics = ["average_distance_session", "total_player_load"]

#MAIN PAGE
#The first row of numbers on the dashboard displaying the chosen date's values 
most_recent_p_vals = ["average_duration_session","total_player_load","average_distance_session",'max_vel',"hsd_yds_70%_<","player_load_per_minute","accel_load_density_index"]

# The table of 7, 14, 21 and 28 day averages with color formating 
rolling_averages_table = ["average_duration_session","total_player_load","average_distance_session","high_speed_running_(avg)","velocity2_band8_total_distance",'velocity2_band8_total_distance','player_load_per_minute']

#The first bar chart with the past month of tagged z-scores for practices of the same tag
practice_z_scores = ["total_player_load","average_distance_session","max_vel","percentage_max_velocity","max_effort_acceleration",'explosive_efforts',"player_load_per_minute","high_speed_running_(avg)","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count","accel_load_density_index"]
# The bar chart displaying tagged z_scores for the current week
weekly_bar_chart = practice_z_scores
# Radar plot
radar_plot = ["total_player_load","average_distance_session","percentage_max_velocity","max_effort_deceleration","max_effort_acceleration", "player_load_per_minute","explosive_efforts_per_minute",]

#The line plot that shows the change in a metrics ACWR over time
team_acwr_graph = ["average_distance_session","total_player_load","max_vel","percentage_max_velocity","high_speed_running_(avg)","explosive_efforts","player_load_per_minute","accel_load_density_index","explosive_efforts_per_minute","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count"]
#Box plot of player distributions for a practice
box_plot = team_acwr_graph

#POSITION OVERVIEW
#Metrics used on the first table on the position overview with color highlighting
position_table = ["average_duration_session","total_player_load","average_distance_session","total_distance_(y)_5mph<",'max_vel',"percentage_max_velocity","velocity2_band8_total_distance","high_speed_running_(avg)","max_effort_acceleration", "max_effort_deceleration","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count","player_load_per_minute","accel_load_density_index","explosive_efforts_per_minute"]
#The second table on the position overiew table with values for each player in a position 
player_table = position_table
#Radar plot (position page only)
pos_radar_plot = ["average_distance_session","total_player_load","percentage_max_velocity",'velocity2_band8_total_distance','velocity2_band8_total_distance',"accel_load_density_index","player_load_per_minute"]
#Box plot (position page only)
pos_group_box_plot = ["total_player_load","average_distance_session",'max_vel',"velocity2_band8_total_distance","high_speed_running_(avg)","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count","player_load_per_minute"]

#INDIVIDUAL OVERVIEW
#The table of most recent practice's metrics, we would want this to be equal to most_recent_p_vals if we wanted it to mirror the home page
individual_table = position_table
#Table with rolling averages, and conditional formatting on ACWRs
individual_acwr_table = ["total_player_load","average_distance_session","average_distance_session",'velocity2_band8_total_distance','velocity2_band8_total_distance',"high_speed_running_(avg)","acceleration_load_average","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count"]

#Ignore this, from the dummy table example on catapult
dummy_table = ["total_player_load","average_distance_session","max_vel","high_speed_running_(avg)","gen2_velocity2_band8_total_effort_count","gen2_acceleration_band7plus_average_effort_count","gen2_acceleration_band2plus_average_effort_count","acceleration_load_average","acceleration_density","explosive_efforts","max_effort_acceleration","gen2_acceleration_band7plus_total_distance","max_effort_deceleration", "gen2_acceleration_band2plus_total_distance","player_load_per_minute","velocity2_band8_total_distance","gen2_velocity2_band8_total_effort_count","hsd_yds_%","total_duration","total_distance_(y)_5mph<"]

needed_vars = set(summary_metrics + intensity_metrics + density_metrics + volume_metrics + ['max_vel',"average_duration_session","total_player_load","average_distance_session","total_acceleration_load","gen2_acceleration_band2plus_average_effort_count"]
+ most_recent_p_vals + rolling_averages_table + practice_z_scores + weekly_bar_chart + radar_plot + team_acwr_graph + position_table + player_table + pos_radar_plot + pos_group_box_plot + 
individual_table + individual_acwr_table + dummy_table)

def metric_checker(x):
    d = dict(zip(x, [i in dummy_table for i in x]))
    for i in x:
        if d[i] == False:
            print(i)
metric_checker(needed_vars)

#print(len(needed_vars))
k = stn.keys()
for i in needed_vars:
    if i not in k:
        print(i+ "not found in slugs")
    #print(f'"{i}",')

range_buttons = dict(
            buttons=list([
                dict(count=7,
                     label="1w",
                     step="day",
                     stepmode="backward"),
                dict(count=14,
                     label="2w",
                     step="day",
                     stepmode="backward"),
                dict(count=21, 
                     label="3w",
                     step="day",
                     stepmode="backward"),
                dict(count=36,
                     label="1m",
                     step="day",
                     stepmode="backward"),
                dict(step="all")
            ])
        )        

acwr_upperbound = 1.2
acwr_lowerbound = .9

intensity_ACs = [] # better way to add to each element in a list?
for met in intensity_metrics: intensity_ACs.append(met + " AC Ratio")

density_ACs = [] # better way to add to each element in a list?
for met in density_metrics: density_ACs.append(met + " AC Ratio")

volume_ACs = [] # better way to add to each element in a list?
for met in volume_metrics: volume_ACs.append(met + " AC Ratio")

summary_28_zscore = []
summary_28_tagged_zscore = []
for met in summary_metrics: summary_28_zscore.append(met + " 28-day Z-score")
for met in summary_metrics: summary_28_tagged_zscore.append(met + " 28-day tagged Z-score")

intensity_28_zscore = []
intensity_28_tagged_zscore = []
for met in intensity_metrics: intensity_28_zscore.append(met + " 28-day Z-score")
for met in intensity_metrics: intensity_28_tagged_zscore.append(met + " 28-day tagged Z-score")

density_28_zscore = []
density_28_tagged_zscore = []
for met in density_metrics: density_28_zscore.append(met + " 28-day Z-score")
for met in density_metrics: density_28_tagged_zscore.append(met + " 28-day tagged Z-score")

volume_28_zscore = []
volume_28_tagged_zscore = []
for met in volume_metrics: volume_28_zscore.append(met + " 28-day Z-score")
for met in volume_metrics: volume_28_tagged_zscore.append(met + " 28-day tagged Z-score")

summary_dict = dict(zip(summary_metrics,summary_metrics_readable)) # I should probably get this from the API and then serialize it 
day_ranges = [7,14,21,28]
z_score_cols = []
summary_ACs = []
ac_dict = {}
for i in summary_metrics:
    z_score_cols.append(i + ' 28-day Z-score')
    summary_ACs.append(i + " AC Ratio")
    ac_dict[i + " AC Ratio"] = summary_dict[i] + " AC Ratio"    
    

def all_dates_curr(date): 
    month = date.month 
    year = date.year
    number_of_days = calendar.monthrange(year, month)[1]
    first_date = dt.date(year, month, 1)
    last_date = dt.date(year, month, number_of_days)
    return pd.date_range(first_date, last_date)
    
    
def dates_month(date, shift):
    if False: #Shift == 0
        month = date.month
        year = date.year
        number_of_days = calendar.monthrange(year, month)[1]
        first_date = dt.date(year, month, 1)
        #last_date = #need to make this selected date
        #return pd.date_range(first_date, last_date)
    else:
        #add year support here
        month = date.month - shift
        year = date.year
        if month <= 0 : 
            month += 12
            year -= 1
            
        number_of_days = calendar.monthrange(year, month)[1]
        first_date = dt.date(year, month, 1)
        last_date = dt.date(year, month, number_of_days)
        return pd.date_range(first_date, last_date)


def normalize_nan(my_array):
    norm = np.sqrt(np.nansum(np.square(my_array)))
    my_array_normalized = my_array/norm
    return my_array_normalized
    
    
def datetime_set_ind(x):
    # Sets index to "date_id" column 
    return x.set_index(pd.to_datetime(x['date_id']),drop=False)
    
    
def give_z(df, var):
    df['temp'] = 1
    orig = list(df.index)
    df = df.resample('D').mean()
    for d in day_ranges:
        df[f"{var} {d}-day tagged Avg"] = df[var].rolling(d, closed = "left", min_periods = d//7*3).mean()
        df[f"{var} {d}-day tagged Z-score"] = df[var] - df[f"{var} {d}-day tagged Avg"]/ df[var].std()
    df = df.loc[orig]
    df = df.drop(columns = 'temp')
    return df 
    
    
def add_tagged_z(df_gb):
    for v in summary_metrics:
        temp = df_gb.groupby("tag").apply(lambda x: give_z(x,v)) 
        
        for d in day_ranges:
            df_gb[f"{v} {d}-day tagged Z-score"] = temp[f"{v} {d}-day tagged Z-score"]
    return df_gb


def add_practice_tag(df,write = False, outpath = None):
    #tag_df = s3_read(tags_csv_path)
    tag_df = pd.read_csv(tags_csv_path)
    data = df
    data['date_id'] = pd.to_datetime(data['date_id'])
    tag_df['date_id'] = pd.to_datetime(tag_df['date_id'])
   
    if 'date_id' in data.columns: date_col = 'date_id'
    data = pd.merge(data, tag_df, on = 'date_id') 
    
    if write: 
        data.to_csv(outpath)
    
    return data

#ight_green = Color('#e1faec')
#light_red = Color('#ffe6e8')

"""
def color_helper(j):
    
#    print("JJJJJJJJJ",j)
#    if type(j) == pd.Series:
#        j = j.item()
    if j < .9:
        return "background-color:" +  str(light_red) + "; opacity : 0.001"
    elif j > 1.2:
        return "background-color: " + str(light_red) + " ; opacity : 0.001"
    return "background-color: #FFFFFF"

"""

