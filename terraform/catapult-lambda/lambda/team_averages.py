import pandas as pd
import numpy as np 
import data_pulling_variables as variables

import config

team_summary_csv_path = config.team_summary_csv_path
position_summary_csv_path = config.position_summary_csv_path

def team_averages(data, to_csv=True):
    # There is a problem with the naming structure. Because we groupby date and take the mean, we average together all the "variable" AC Ratios, as defined in variables.summary_ACs.
    # This is different to the teams, because the number of players in practice change, so it is not equal to the AC ratio for each practices variable's values.
    # So, we recalculate the AC Ratio values, based on each variables average for each day, and then store them as variable + "absolute ACWR". 
    # These are the columns to be used when calculating data, and displaying team or position summary data. Individual data should call "AC Ratio", which is defined in variables 

    #data = pd.read_csv('tmp/Data with AC 3.csv')
    
    print(sum(data['tag_0'].isna()))
    data['tag_0'] = data['tag_0'].fillna(0)
    print(sum(data['tag_0'].isna()))
    print("0" ,data['tag_0'].unique())
    
    #data = data[data['tag_0'].apply(lambda x: x in ["Full","Starter"])] #only include full practice players
    data = data[data['tag_0'].isna() | data['tag_0'].isin(['Full','Starter'])] #assume blank values are full 

    data = data.loc[:,['date_id','tag','tag_0','position_name'] + list(variables.needed_vars)]
    #data['average_duration'] = data['average_duration']/60
    
    print("ONE" ,data['tag'].unique())
    def add_team_metrics(data:pd.DataFrame, outpath, position = ''):
        print("in team metrics" + data['tag'].unique())
        #print(data['tag'])
        def gb_fun(x):
            #print(x)
            x['tag'] = x['tag'].mode()
            x['tag_0'] = x['tag_0'].mode()
            return pd.Series.mean(x)

        tmp = data.groupby("date_id").mean()# .apply(lambda x : np.mean(x,))
        def agg_mode(x):
            m = pd.Series.mode(x)
            if len(m) > 1:
                return m[0]
            else: return m
        tags = data.groupby('date_id')[['tag','tag_0']].agg(agg_mode)
        
        tmp['tag'] = tags['tag']
        tmp['tag_0'] = tags['tag_0']
        #tmp['tag_1'] = tags['tag_1']
        data = tmp
        print(data)

        print("1" + tmp['tag'])
        data.to_csv("tmp/tmp-gb.csv")
        data['first_reind'] = 1
        #data.index = data.index.droplevel(1)
        print(data.index)
        print(data)
        data.index = pd.to_datetime(data.index)
        
        data = data.resample('D').asfreq()
        print("1" + data['tag'])
        #print(data)
        
        for v in variables.needed_vars:
            curr = data[f'{v}']        
            for d in variables.day_ranges:
                data[f'{v} {d}-day {position}Z-score'] = (curr - curr.rolling(d,closed='left',min_periods=1).mean())/curr.std() #rolling(7,closed='left',min_periods=2).std()
                data[f'{v} {d}-day {position}Avg'] = curr.rolling(d,closed='left',min_periods=1).apply(lambda x : np.mean(x))

                #data[f'{v} 28-day {position}Z-score'] = (curr - curr.rolling(28,closed='left',min_periods=1).mean())/curr.std() #rolling(7,closed='left',min_periods=2).std()
            data[f'{v} {position}Z-score'] =  (curr - curr.mean())/curr.std()
            data[f'{v} {position}absolute ACWR'] =  curr /curr.rolling(28, closed='left',min_periods=1).apply(lambda x : np.mean(x))
        print("2" + data['tag'])
        
        data = data.replace([float('inf'),float('-inf')],np.nan)
        dates = data.index
        data = data.reset_index()
        print("3" + data['tag'])

        data['date_id'] = dates
        data = data[data['first_reind'] == 1] 
        data = data.drop(columns='first_reind')
        print("4" + data['tag'])
        
        #data = variables.add_practice_tag(data)
    
        #data['tag_'] = data['tag']

        all_tagged_ACWRs = []
        all_absolute_ACWRs = list(variables.summary_ACs)

        density_tagged_ACWRs = []
        density_absolute_ACWRs = []
        for v in variables.density_metrics:
            density_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            all_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            density_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
            all_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
        
        intensity_absolute_ACWRs = []
        intensity_tagged_ACWRs = []
        for v in variables.intensity_metrics:
            intensity_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            all_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            intensity_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
            all_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
        
        volume_absolute_ACWRs = []
        volume_tagged_ACWRs = []
        for v in variables.volume_metrics:
            volume_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            all_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            volume_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
            all_absolute_ACWRs.append(v +  f" {position}absolute ACWR")

        summary_absolute_ACWRs = []
        summary_tagged_ACWRs = []
        for v in variables.summary_metrics:
            summary_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            all_tagged_ACWRs.append(v +f" tagged uncoupled {position}ACWR")
            summary_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
            all_absolute_ACWRs.append(v +  f" {position}absolute ACWR")
        print("5" + data['tag'])
        
        def find_z(df:pd.DataFrame):
           # print("FINNNNNNNNNNDDDDDDDDD ZZZZZZZZZZZZZZZZZZZz")
            df = df.set_index("date_id")
            start_ind = df.index
            df.sort_index(ascending=True,inplace=True)

            df = df.resample("D").mean()
            #print(df)
            for v in variables.needed_vars:        
                curr = df[f'{v}']
                for i in variables.day_ranges: 
                    if i <= 7: continue  
                    df[f'{v} {i}-day tagged {position}Z-score'] = (curr - curr.rolling(i,closed='left',min_periods=1).apply(lambda x : np.mean(x)))/curr.std() #.rolling(i,closed='left',min_periods=2).apply(lambda x : np.std(x))
                    df[f'{v} {i}-day tagged {position}Avg'] = curr.rolling(i,closed='left',min_periods=1).apply(lambda x : np.mean(x))
                    #make these empty columns first
                df[f"{v} tagged uncoupled {position}ACWR"] = curr /curr.rolling(28, closed='left',min_periods=1).apply(lambda x : np.mean(x))
            df = df.loc[start_ind,:]
            
            return df
        
        print('before tag groupby')
        print(data['tag'].unique())
        data = data.groupby("tag").apply(find_z)
        print('after tag groupby')
        #print(data.index)
        print('1.',data)
        data.sort_index(level=1,ascending=True,inplace=True)
        print('2.',data)
        #tags = data.index.droplevel(1)
        #print(tags)
        data.reset_index(inplace=True)
        print('3.',data)
        
        #summary_28_zscore
        #summary_28_tagged_zscore
        
        summary_28_z_score = []
        summary_28_tagged_z_score = []
        for v in variables.summary_metrics:
            summary_28_z_score.append(v + f" 28-day {position}Z-score")
            summary_28_tagged_z_score.append(v + f" 28-day tagged {position}Z-score")
        
        for i in summary_28_tagged_z_score:
            print(i)
            print(i in data.columns)
        data[f'{position}summary_z_avg'] = data.loc[:,summary_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}summary_tagged_z_avg'] = data.loc[:,summary_28_tagged_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}summary tagged uncoupled ACWR'] = data.loc[:,summary_tagged_ACWRs].mean(axis=1,numeric_only=True,skipna=True)
        #data.to_csv('tmp/tmp.csv')
        '''
        if position == '':
            print(outpath)
            print(data.loc[max(data.index),variables.summary_ACs]) 
            summary_ACWRs = [i + ' absolute ACWR' for i in variables.summary_metrics]   
            data[f'{position}summary absolute ACWR'] = data.loc[:,summary_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        else:
            cols = [i + ' position absolute ACWR' for i in variables.summary_metrics]
            data[f'{position}summary absolute ACWR'] = data.loc[:,cols].mean(axis=1,numeric_only=True,skipna=True)    
        '''
        
        data[f'{position}summary absolute ACWR'] = data.loc[:,summary_absolute_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        
        intensity_28_z_score = []
        intensity_28_tagged_z_score = []
        for v in variables.intensity_metrics:
            intensity_28_z_score.append(v + f" 28-day {position}Z-score")
            intensity_28_tagged_z_score.append(v + f" 28-day tagged {position}Z-score")
        
        data[f'{position}intensity_z_avg'] = data.loc[:,intensity_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}intensity_tagged_z_avg'] = data.loc[:,intensity_28_tagged_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}intensity tagged uncoupled ACWR'] = data.loc[:,intensity_tagged_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        data[f'{position}intensity absolute ACWR'] = data.loc[:,intensity_absolute_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        
        density_28_z_score = []
        density_28_tagged_z_score = []
        for v in variables.density_metrics:
            density_28_z_score.append(v + f" 28-day {position}Z-score")
            density_28_tagged_z_score.append(v + f" 28-day tagged {position}Z-score")

        data[f'{position}density_z_avg'] = data.loc[:,density_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}density_tagged_z_avg'] = data.loc[:,density_28_tagged_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}density tagged uncoupled ACWR'] = data.loc[:,density_tagged_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        data[f'{position}density absolute ACWR'] = data.loc[:,density_absolute_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        
        volume_28_z_score = []
        volume_28_tagged_z_score = []
        for v in variables.volume_metrics:
            volume_28_z_score.append(v + f" 28-day {position}Z-score")
            volume_28_tagged_z_score.append(v + f" 28-day tagged {position}Z-score")

        data[f'{position}volume_z_avg'] = data.loc[:,volume_28_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}volume_tagged_z_avg'] = data.loc[:,volume_28_tagged_z_score].mean(axis=1,numeric_only=True,skipna=True)
        data[f'{position}volume tagged uncoupled ACWR'] = data.loc[:,volume_tagged_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        data[f'{position}volume absolute ACWR'] = data.loc[:,volume_absolute_ACWRs].mean(axis=1,numeric_only=True,skipna=True)    
        
        #data['tag'] = tags
        print('dt \n \n',data['tag'])
        data = data.set_index('date_id')
        #data.insert(0,'tag_',tags)
        
        data_max = data.resample("D").max()
        data = data_max
        
        print(data)
        if to_csv:
            data.to_csv(outpath)
        return data
        
        ''' 
        hscs = ["date_id",'tag'] # part of the index now
        pl  = list(hscs)
        for i in data.columns:
            if "high_speed" in i:
                hscs.append(i)
            if "player_load" in i:
                pl.append(i)

        hs_data  = data.loc[:,hscs]
        
        print(data["high_speed_running 7-day Z-score"][0:10])
        print(data["high_speed_running 28-day tagged Avg"][0:10])

        #print(data["high_speed_running"][0:10])
        hs_data.to_csv("tmp/highspeed.csv")
        pl_data = data.loc[:,pl]
        pl_data.to_csv("tmp/pl.csv")
        '''

    def add_position_metrics(df:pd.DataFrame):
        print('in add position metrics',print(df['tag']))
        tmp = df.groupby("date_id").mean()# .apply(lambda x : np.mean(x,))
        def agg_mode(x):
            m = pd.Series.mode(x)
            if len(m) > 1:
                return m[0]
            else: return m
        tags = df.groupby('date_id')[['tag','tag_0']].agg(agg_mode)
        print(tags['tag'])
        tmp['tag'] = tags['tag']
        tmp['tag_0'] = tags['tag_0']
       #tmp['tag_1'] = tags['tag_1']
        df = tmp
        print(df['tag'])
        print(df['tag'].unique())

        df = add_team_metrics(df,'tmp.csv','position ')
        return df
        
    team_summary = add_team_metrics(data, team_summary_csv_path) 
    '''
    def agg_mode(x):
        m = pd.Series.mode(x)
        if len(m) > 1:
            return m[0]
        else: return m
    tags = data.groupby('date_id')[['tag','tag_0','tag_1']].agg(agg_mode)
    data['tag'] = tags['tag']
    data['tag_0'] = tags['tag_0']
    data['tag_1'] = tags['tag_1']
    '''
    print('before pm', data['tag'])
    position_metrics =  data.groupby("position_name").apply(lambda x : add_position_metrics(x))
    
    print(position_metrics)

    if to_csv:
        position_metrics.to_csv(position_summary_csv_path)
    return team_summary, position_metrics
    
    