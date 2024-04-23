import pandas as pd
import numpy as np
import re
import data_pulling_variables as variables

import config

#data = pd.read_csv('curr_data.csv')

#x.insert(0,0)

tags_csv_path = config.tags_csv_path
final_data_path = config.final_data_path
j = 0 
i = 0


def add_practice_tag(data, outpath = ''):
    tag_df = pd.read_csv(tags_csv_path)
    if 'tag' in data.columns:
        data = data.drop(columns = ['tag'])

    data['date_id'] = pd.to_datetime(data['date_id'])
    tag_df['date_id'] = pd.to_datetime(tag_df['date_id'])
    #print(tag_df['date_id'].dtype)
    #print(data['date_id'].dtype)
    
    #date_col = ''
    #if 'date_id' in data.columns: date_col = 'date_id'
    data = pd.merge(data, tag_df, on = 'date_id') 
    #print(data)
    if outpath != None and outpath != '':
        data.to_csv(outpath,index=False)
    return data
    
    
def add_rolling_metrics(data, cols_present = False,to_csv = True):
    
    # DO ANY TRANSFORMATIONS HERE!
    data['average_duration_session'] = data['average_duration_session']/60
    data.sort_values(by='total_player_load',axis=0,ascending=False,inplace=True)

    data.drop_duplicates(subset=['athlete_name','date_id'],inplace=True)
    data.sort_values(by='date_id',axis=0,ascending=True,inplace=True)
    
    #data['average_duration_session'] = data['average_duration_session'] / 60
    if not 'tag' in data.columns: print("no tag in data")
    if not cols_present:
        #desired_rollings = ['hsd_yds_%', 'hsd_yds_70%_<', 'explosive_efforts', 'gen2_acceleration_band2plus_total_effort_count', 'gen2_acceleration_band7plus_total_effort_count', 'gen2_acceleration_band2plus_total_distance', 'gen2_acceleration_band7plus_total_distance', 'max_effort_acceleration', 'max_effort_deceleration', 'acceleration_density', 'total_acceleration_load', 'player_load_per_minute', 'total_distance_(y)_5mph<', 'total_distance', 'total_duration', 'total_player_load', 'max_vel', 'velocity2_band8_total_distance', 'gen2_velocity2_band8_total_effort_count', 'high_speed_running','average_duration_session']
        new_cols = []
        days = ['7','14','21','28']
        for metric in variables.needed_vars:
            assert metric in data.columns, f"{metric} not present in base data"
            for day in days:
                new_cols.append(f"{metric} {day}-day Avg")
                new_cols.append(f"{metric} {day}-day STDV")
                new_cols.append(f"{metric} {day}-day Z-score")
                #if metric in summary_metrics:
                #    new_cols.append(f"{metric} {day}-day Avg")
        print(len(data.columns.unique()) == len(data.columns))
        data = data.reindex(columns = set(data.columns.tolist() + new_cols))
    print(f"RAW DATA PRESENT: {'total_player_load' in data.columns}")
    
    def athlete_metrics(df_gb:pd.DataFrame):
        #print(f"ATHLETE METRICS tag_0 present =={'tag_0' in data.columns}")
        global j
        #probably would be better to not add columns like this and instead go from list of metrics
        j+= 1
        print(j)
        #print("j:",j)
        if j%10 == 0:
            print(f"{j} athletes processed")
        #takes in a group_by athletes dataframe and calculates the correct metrics for each column within that dataframe, so that this function can be used in groupby apply
        #df_gb = add_practice_tag(df_gb) #.to_csv("df_gb_where_tag.csv")
        #print('df_gb', df_gb)
        to_drop = [] #Tracks duplicate columns
        dont_drop = []
        #print(len(df_gb['date_id']))
        #print(len(df_gb['date_id'].unique()))
        #if len(df_gb['date_id']) != len(df_gb['date_id'].unique()):
            #for
        df_gb = df_gb.set_index('date_id')
        
        df_gb.index = pd.to_datetime(df_gb.index) 
       
        def resample_fun(x):
            #print(x)
            #print(type(x))
            return pd.Series.max(x)
        
        df_gb = df_gb.resample('D').asfreq()
        df_gb.reset_index(inplace=True)
        cols = df_gb.columns
        #if not 'tag' in cols: print('no tags in df_gb') 
        
        for col in cols:
            if "Avg" in col:
                
                splt = col.split(" ")
                days = int(re.match("(\\d+)", splt[-2])[0])
                #base_col is the column with the original data, from which we are finding the ewm 
                base_col = ' '.join(splt[:-2])
                if base_col in cols:
                    r_l = list(df_gb[base_col].ewm(span = days, ignore_na = False).mean())[:-1]
                    r_l.insert(0,0)
                    df_gb[col] = r_l
                    if j == 1:
                       1==1 
                       # print(df_gb[col])
                to_drop.append(col + "_x")
                
            elif "STDV" in col:
                splt = col.split(" ")
                days = int(re.match("(\\d+)", splt[-2])[0])
                base_col = ' '.join(splt[:-2])
                if base_col in cols:
                    #min periods is equal to 7//3 * 1: 3 for 7, 6 for 14...
                    df_gb[col] = df_gb[base_col].rolling(days, closed = "left", min_periods = 1).apply(lambda x : np.std(x, ddof=0))
                #to_drop.append(col + "_x")
                #dont_drop.append(col+"_y")
            ''' THIS SHOULDN'T BE NEEDED ANYMORE
            elif "Avg" in col:
                splt = col.split(" ")
                days = int(re.match("(\\d+)", splt[-2])[0])
                base_col = ' '.join(splt[:-2])
                if base_col in cols:
                    #min periods is equal to 7//3 * 1: 3 for 7, 6 for 14...
                    df_gb[col] = df_gb[base_col].rolling(days, closed = "left", min_periods = 1).mean()
                #to_drop.append(col + "_x")
                #dont_drop.append(col+"_y")
            '''
        for col in cols:
            if "Z-score" in col:
                splt = col.split(" ")
                days = int(re.match("(\\d+)", splt[-2])[0])
                base_col = ' '.join(splt[:-2])
                stdv_col = f"{base_col} {days}-day STDV" 
                
                df_gb[col] = (df_gb[base_col] - df_gb[base_col].rolling(days, closed = "left", min_periods = 1).mean()) / df_gb[stdv_col]
                
                #to_drop.append(col + "_x")
                #dont_drop.append(col+"_y")

        def find_z(df:pd.DataFrame):
            df = df.set_index("date_id")
            start_ind = df.index
            df.sort_index(ascending=True,inplace=True)

            df = df.resample("D").mean()
            #print(df)
            for v in variables.needed_vars:        
                curr = df[f'{v}']
                for i in variables.day_ranges: 
                    if i <= 7: continue  
                    df[f'{v} {i}-day tagged Z-score'] = (curr - curr.rolling(i,closed='left',min_periods=1).apply(lambda x : np.mean(x)))/curr.std() #.rolling(i,closed='left',min_periods=2).apply(lambda x : np.std(x))
                    df[f'{v} {i}-day tagged Avg'] = curr.rolling(i,closed='left',min_periods=1).apply(lambda x : np.mean(x))
                    #make these empty columns first
                df[f"{v} tagged uncoupled ACWR"] = curr /curr.rolling(28, closed='left',min_periods=1).apply(lambda x : np.mean(x))
            df = df.loc[start_ind,:]
            
            return df
        #df_gb.to_csv('df_gb_where_tag.csv')
        #print(df_gb['date_id'])
        #df_gb = add_practice_tag(df_gb)
        #print("adding tags dfgb:",df_gb['tag'])

        df_gb = df_gb.groupby("tag").apply(lambda x:find_z(x))
        #We lose the availability tags with the groupby above 
        #  print('after tag gb', df_gb['tag_0'])
        df_gb.reset_index(inplace=True)
        df_gb.set_index('date_id', inplace=True)
        df_gb = df_gb.resample('D').max()
        
        return df_gb

    #filters the data for testing purposes
    #names = data['athlete_name'].unique()[0:10]
    #data = data[data['athlete_name'].apply(lambda x : x in names)]
    
    #print(data)
    ''''''
    ls = data['date_id'] + ' ' + data['athlete_name']
    un = ls.unique()
    ls = list(ls)
    for i in un:
        if ls.count(i) > 1:
            #print(i)
            pass
    with_new_metrics = data.groupby("athlete_name").apply(lambda x : athlete_metrics(x)) #choose which function to use here 
    #print(with_new_metrics)
    with_new_metrics = with_new_metrics.reorder_levels(["date_id", "athlete_name"]).sort_index(axis = 0,level = "date_id")
    
    with_new_metrics = with_new_metrics.reset_index(["date_id","athlete_name"])
    #with_new_metrics.to_csv("no_ac.csv")
    print(f"TWO tag_0 present =={'tag_0' in with_new_metrics.columns}")

    i = 0
    def add_AC_ratio_cols(df_gb):
        global i
        i += 1
        if i%10 == 0:
            print(i + " completed")
        df_gb = pd.DataFrame(df_gb)
        for c in variables.needed_vars:
            roll_28 = c + " 28-day Avg"
            roll_7 = c + " 7-day Avg"
            col_name = str(c) + " AC Ratio"
            #print(df_gb[roll_7])
            df_gb[f"{col_name}"] = df_gb[roll_7] /df_gb[roll_28]
        return df_gb
        
    def flagging_func(ser):
        #just return 1 if overtrained, -1 if undertrained
        pos_score = 0 
        neg_score = 0
        for i in ser:
            #if i > 1.5: #arbitrary
            #  pos_score += 2
            if i > 1.3:
                pos_score += 1
            elif i < .8: #Arbitrary
                neg_score -= 1
            #elif i < .8:
            #    neg_score -= 1
        if pos_score == neg_score == 0:
            return 0
        if pos_score >1 and neg_score > 1:
            return 'nan'
        if pos_score > abs(neg_score):
            return 1
        
        return -1

    '''
    intensity = ['max_effort_acceleration AC Ratio','max_effort_deceleration AC Ratio','total_acceleration_load AC Ratio','player_load_per_minute AC Ratio','hsd_yds_% AC Ratio', 'hsd_yds_70%_< AC Ratio']
    density = ['acceleration_density AC Ratio','player_load_per_minute AC Ratio','explosive_efforts AC Ratio']
    volume = ['gen2_acceleration_band2plus_total_effort_count AC Ratio',
        'gen2_acceleration_band7plus_total_effort_count AC Ratio',
        'gen2_acceleration_band2plus_total_distance AC Ratio',
        'gen2_acceleration_band7plus_total_distance AC Ratio'] #,'hsd_yds_% AC Ratio', 'hsd_yds_70%_< AC Ratio']
    all_metrics = [] + intensity + density + volume
    '''
    def out_of_range_sum(ser):
        tot = 0
        for i in ser:
            if i > variables.acwr_upperbound:
                tot += i - variables.acwr_upperbound
            elif i < variables.acwr_lowerbound:
                tot += variables.acwr_lowerbound - i
        return tot
    #print('With new metrics contains tag_0', 'tag_0' in with_new_metrics.columns)
    
    x  = add_AC_ratio_cols(with_new_metrics)
    # From testing how to not lose tagged columns when calculating AC metrics, this is not used anymore because we merge with tag info after calculating metrics
    #print(f"RAW DATA PRESENT WITH NEW METRICS: {'total_player_load' in with_new_metrics.columns}")
    #print(f"RAW DATA PRESENT X: {'total_player_load' in x.columns}")
    #print(f"Three tag_0 present =={'tag_0' in x.columns}")

    data_intensity = x.loc[:,variables.intensity_metrics]
    data_density = x.loc[:,variables.density_metrics]
    data_volume = x.loc[:,variables.volume_metrics]
    data_metrics = x.loc[:,variables.needed_vars]
   
    def problem_metrics(ser):
        num = 0
        for i in ser:
            if not variables.acwr_lowerbound < i < variables.acwr_upperbound:
                num += 1
        return num
        
    x["intensity_flag"] = data_intensity.apply(flagging_func, axis=1)
    x["density_flag"] = data_density.apply(flagging_func, axis=1)
    x["volume_flag"] = data_volume.apply(flagging_func, axis=1)
    x['all_flag'] = x.loc[:,["intensity_flag", "density_flag", "volume_flag"]].mean(axis=1).round(2)
    x['abs_flag'] = x.loc[:,["intensity_flag", "density_flag", "volume_flag"]].abs().mean(axis=1).round(2)
    x["problem_metrics"] = x.loc[:,variables.summary_ACs].apply(problem_metrics, axis=1)
    
    x['out_of_range_avg'] = x.loc[:,variables.summary_ACs].apply(out_of_range_sum, axis=1)/len(variables.summary_ACs)
    x["intensity_out_of_range"] = x.loc[:,variables.intensity_ACs].apply(out_of_range_sum, axis=1)/len(variables.intensity_ACs)
    x["density_out_of_range"] = x.loc[:,variables.density_ACs].apply(out_of_range_sum, axis=1)/len(variables.density_ACs)
    x["volume_out_of_range"] = x.loc[:,variables.volume_ACs].apply(out_of_range_sum, axis=1)/len(variables.volume_ACs)
    
    #tagged col names in data: " tagged uncoupled ACWR"
    #untagged col names in data: " AC Ratio"
    
    #tagged col names in calendar generator: " tagged uncoupled ACWR"
    #untagged col names in calendar generator: " absolute ACWR"
    
    summary_28_z_score = []
    summary_tagged_ac = []
    summary_ac = []
    for v in variables.summary_metrics: 
        summary_28_z_score.append(v + f" 28-day Z-score")
        summary_tagged_ac.append(v + " tagged uncoupled ACWR")
        summary_ac.append(v + " AC Ratio")
    x[f'summary_z_avg'] = x.loc[:,summary_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
    x['summary tagged uncoupled ACWR'] = x.loc[:,summary_tagged_ac].mean(axis=1,numeric_only=True,skipna=True)
    x['summary absolute ACWR'] = x.loc[:,summary_ac].mean(axis=1,numeric_only=True,skipna=True)
    
    volume_28_z_score = []
    volume_tagged_ac = []
    volume_ac = []
    for v in variables.volume_metrics: 
        volume_28_z_score.append(v + f" 28-day Z-score")
        volume_tagged_ac.append(v + " tagged uncoupled ACWR")
        volume_ac.append(v + " AC Ratio")
    x[f'volume_z_avg'] = x.loc[:,volume_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
    x['volume tagged uncoupled ACWR'] = x.loc[:,volume_tagged_ac].mean(axis=1,numeric_only=True,skipna=True)
    x['volume absolute ACWR'] = x.loc[:,volume_ac].mean(axis=1,numeric_only=True,skipna=True)
    
    density_28_z_score = []
    density_tagged_ac = []
    density_ac = []
    for v in variables.density_metrics: 
        density_28_z_score.append(v + f" 28-day Z-score")
        density_tagged_ac.append(v + " tagged uncoupled ACWR")
        density_ac.append(v + " AC Ratio")
    x[f'density_z_avg'] = x.loc[:,density_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
    x['density tagged uncoupled ACWR'] = x.loc[:,density_tagged_ac].mean(axis=1,numeric_only=True,skipna=True)
    x['density absolute ACWR'] = x.loc[:,density_ac].mean(axis=1,numeric_only=True,skipna=True)

    intensity_28_z_score = []
    intensity_tagged_ac = []
    intensity_ac = []
    for v in variables.intensity_metrics: 
        intensity_28_z_score.append(v + f" 28-day Z-score")
        intensity_tagged_ac.append(v + " tagged uncoupled ACWR")
        intensity_ac.append(v + " AC Ratio")
    x[f'intensity_z_avg'] = x.loc[:,intensity_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
    x['intensity tagged uncoupled ACWR'] = x.loc[:,intensity_tagged_ac].mean(axis=1,numeric_only=True,skipna=True)
    x['intensity absolute ACWR'] = x.loc[:,intensity_ac].mean(axis=1,numeric_only=True,skipna=True)

    #variables.position_name_group_map(x) #in place
   
    if to_csv: x.to_csv('Data With AC 3.csv')   
    return x
    #add_practice_tag(x,outpath='Data With AC 3.csv')
    
#add_rolling_metrics(pd.read_csv("tagged_no_ac_data.csv"))

#activities = pd.read_csv('merge_with_activities.csv')
#w_tag = add_practice_tag(activities, "merge_with_activities.csv")#add_rolling_metrics(pd.read_csv('activities22.csv'))

#print(w_tag)
#add_rolling_metrics(w_tag)

#add_rolling_metrics(pd.read_csv("tagged_no_ac_data.csv"))

#data = pd.read_csv("Data With AC 3.csv")
