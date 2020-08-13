# -*- coding: utf-8 -*-
"""
Created on Thu Aug 13 19:21:59 2020

@author: Const
"""

from google.oauth2 import service_account
import pandas_gbq 

import numpy as np
import pandas as pd
import math as mt
import datetime as dt

CREDENTIALS = service_account.Credentials.from_service_account_info({***})

def getFreshData(Credentials,ProjectId):
    bigquery_sql = " ".join(["SELECT id, DATE(CAST(created_at AS DATETIME)) AS created, DATE(CAST(updated_at AS DATETIME)) AS updated, status, assignee_id, channel",
                             "FROM `xsolla_summer_school.customer_support`",
                             "WHERE status IN ('closed','solved')",
                             "ORDER BY updated_at"])

    dataframe = pandas_gbq.read_gbq(bigquery_sql,project_id=ProjectId, credentials=Credentials, dialect="standard")

    return dataframe

'''Только для статусов'''
def workloadScoringByStatuses(Data,NumOfAllDays,NumOfIntervalDays):
    assignee_id = np.unique(Data.assignee_id)
    assignee_id = assignee_id[0]
    
    #splitting by status
    statuses = np.unique(Data.status)
    assignee_id_list = []
    status_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []
    for status in statuses:
        dataframe_status = Data[(Data.status == str(status))][:]
    
        #time borders params
        curr_date = dt.datetime.strptime(str('2018-04-01'),'%Y-%m-%d')
        curr_date = curr_date.date()
        delta = dt.timedelta(days=NumOfAllDays)
        first_date = curr_date-delta
    
        #time interval params
        delta_interval = dt.timedelta(days=NumOfIntervalDays)
        first_interval = first_date+delta_interval
            
        num_of_intervals = int(NumOfAllDays/NumOfIntervalDays)
        num_tasks_per_week = []
        for i in range(0,num_of_intervals):
            interval = dataframe_status[(dataframe_status.updated >= str(first_date)) & (dataframe_status.updated <= str(first_interval))][:]
            first_date = first_date + delta_interval
            first_interval = first_interval + delta_interval
    
            if i != (num_of_intervals-1):        
                num_of_tasks = len(np.unique(interval['id']))
                num_tasks_per_week.append(num_of_tasks) #history number of tasks
            else:
                num_tasks_per_current_week = len(np.unique(interval['id'])) #currently number of tasks
        
        avg_num_of_task_per_week = round(np.mean(num_tasks_per_week),2)

        #squared deviations
        x_values = []
        for num in num_tasks_per_week:
            x = round((num - avg_num_of_task_per_week)**2,2)
            x_values.append(x)

        #data sampling statistics
        x_sum = round(sum(x_values),2) #sum of squared deviations
        dispersion = round(x_sum/(num_of_intervals-1),2) #dispersion
        std = round(mt.sqrt(dispersion),2) #standart deviation for sample
        ste = round(std/mt.sqrt(num_of_intervals),2) #standart error for sample

        #confidence interval
        left_border = int(avg_num_of_task_per_week - ste)
        right_border = int(avg_num_of_task_per_week + ste)

        #workload scoring for status
        score_for_status = workloadScoreStatuses(left_border,right_border,num_tasks_per_current_week)        
        assignee_id_list.append(assignee_id)
        status_list.append(status)
        avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
        ste_list.append(ste)
        num_tasks_per_current_week_list.append(num_tasks_per_current_week)
        score_for_status_list.append(score_for_status)
        
    score_data = {"assignee_id":assignee_id_list,"status":status_list,
                  "count_last_period":num_tasks_per_current_week_list,"count_mean_calc_period":avg_num_of_task_per_week_list,"count_sem_calc_period":ste_list,
                  "score_value":score_for_status_list}
    scores = pd.DataFrame(data=score_data)

    return scores

'''Для статусов и каналов'''
def workloadScoringByStatusesChannel(Data,NumOfAllDays,NumOfIntervalDays):
    assignee_id = np.unique(Data.assignee_id)
    assignee_id = assignee_id[0]
    
    #splitting by status
    statuses = np.unique(Data.status)
    channels = np.unique(Data.channel.fillna('None'))
    assignee_id_list = []
    status_list = []
    channel_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []
    score_for_channel_list = []
    for channel in channels:
        dataframe_channel = Data[(Data.channel == str(channel))][:]
        for status in statuses:
            dataframe_status = Data[(Data.status == str(status))][:]

            #time borders params
            curr_date = dt.datetime.strptime(str('2018-04-01'),'%Y-%m-%d')
            curr_date = curr_date.date()
            delta = dt.timedelta(days=NumOfAllDays)
            first_date = curr_date-delta

            #time interval params
            delta_interval = dt.timedelta(days=NumOfIntervalDays)
            first_interval = first_date+delta_interval

            num_of_intervals = int(NumOfAllDays/NumOfIntervalDays)
            num_tasks_per_week = []
            for i in range(0,num_of_intervals):
                interval = dataframe_status[(dataframe_status.updated >= str(first_date)) & (dataframe_status.updated <= str(first_interval))][:]
                first_date = first_date + delta_interval
                first_interval = first_interval + delta_interval

                if i != (num_of_intervals-1):        
                    num_of_tasks = len(np.unique(interval['id']))
                    num_tasks_per_week.append(num_of_tasks) #history number of tasks
                else:
                    num_tasks_per_current_week = len(np.unique(interval['id'])) #currently number of tasks

            avg_num_of_task_per_week = round(np.mean(num_tasks_per_week),2)

            #squared deviations
            x_values = []
            for num in num_tasks_per_week:
                x = round((num - avg_num_of_task_per_week)**2,2)
                x_values.append(x)

            #data sampling statistics
            x_sum = round(sum(x_values),2) #sum of squared deviations
            dispersion = round(x_sum/(num_of_intervals-1),2) #dispersion
            std = round(mt.sqrt(dispersion),2) #standart deviation for sample
            ste = round(std/mt.sqrt(num_of_intervals),2) #standart error for sample

            #confidence interval
            left_border = int(avg_num_of_task_per_week - ste)
            right_border = int(avg_num_of_task_per_week + ste)

            #workload scoring for status
            score_for_status = workloadScoreStatuses(left_border,right_border,num_tasks_per_current_week)
            assignee_id_list.append(assignee_id)
            status_list.append(status)
            channel_list.append(channel)
            avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
            ste_list.append(ste)
            num_tasks_per_current_week_list.append(num_tasks_per_current_week)
            score_for_status_list.append(score_for_status)
        
    score_data = {"assignee_id":assignee_id_list,"status":status_list, "channel": channel_list,
                  "count_last_period":num_tasks_per_current_week_list,"count_mean_calc_period":avg_num_of_task_per_week_list,"count_sem_calc_period":ste_list,
                  "score_value":score_for_status_list}
    scores = pd.DataFrame(data=score_data)

    return scores

def workloadScoreStatuses(LeftBoard,RightBoard,CurrentNumOfTasks):
    if (LeftBoard == 0) & (CurrentNumOfTasks == 0) & (RightBoard == 0):
        score = 0
    elif (CurrentNumOfTasks >= 0) & (CurrentNumOfTasks < LeftBoard):
        score = 0
    elif (CurrentNumOfTasks >= LeftBoard) & (CurrentNumOfTasks <= RightBoard):
        score = 1
    else:
        score = 2
    
    return score


"Выгрузка данных по статусам"
def insertScoreResultData(InsertDataFrame,ProjectId,DatasetId,TableId):
    destination_table = f"{DatasetId}.{TableId}"
    
    res_df = pd.DataFrame()
    res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int64')
    res_df['status'] = InsertDataFrame['status'].astype('str')
    res_df['count_last_period'] = InsertDataFrame['count_last_period'].astype('int')
    res_df['count_mean_calc_period'] = InsertDataFrame['count_mean_calc_period'].astype('float')
    res_df['count_sem_calc_period'] = InsertDataFrame['count_sem_calc_period'].astype('float')
    res_df['score_value'] = InsertDataFrame['score_value'].astype('int')
    res_df['developer'] = 'konstantin.fadeev'
    res_df['developer'] = res_df['developer'].astype('str')
    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')
   
"Выгрузка по total score"    
def insertScoreResultTotalData(InsertDataFrame,ProjectId,DatasetId,TableId):
    destination_table = f"{DatasetId}.{TableId}"
    
    res_df = pd.DataFrame()
    res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int64')
    res_df['score_value'] = InsertDataFrame['score_total'].astype('float')
    res_df['developer'] = 'konstantin.fadeev'
    res_df['developer'] = res_df['developer'].astype('str')
    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')

"Выгрузка по статусам и каналам"
def insertScoreResultDataChannel(InsertDataFrame,ProjectId,DatasetId,TableId):
    destination_table = f"{DatasetId}.{TableId}"
    
    res_df = pd.DataFrame()
    res_df['assignee_id'] = InsertDataFrame['assignee_id'].astype('int64')
    res_df['status'] = InsertDataFrame['status'].astype('str')
    res_df['channel'] = InsertDataFrame['channel'].astype('str')
    res_df['count_last_period'] = InsertDataFrame['count_last_period'].astype('int')
    res_df['count_mean_calc_period'] = InsertDataFrame['count_mean_calc_period'].astype('float')
    res_df['count_sem_calc_period'] = InsertDataFrame['count_sem_calc_period'].astype('float')
    res_df['score_value'] = InsertDataFrame['score_value'].astype('int')
    res_df['developer'] = 'konstantin.fadeev'
    res_df['developer'] = res_df['developer'].astype('str')
    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')
    


print('Ребятушки - козлятушки')    
DataFrame = getFreshData(CREDENTIALS,'findcsystem')
assignee_id = DataFrame.assignee_id.unique()

'''Обработка загруженности по статусам и выгрузка'''
df_status = pd.DataFrame()
for i in assignee_id:
    df = workloadScoringByStatuses(DataFrame[DataFrame.assignee_id == i][:],63,7)
    df_status = pd.concat([df_status,df])
    
df_status = df_status.reset_index(drop=True)

insertScoreResultData(df_status,'findcsystem','xsolla_summer_school','score_result_status')
print('Пирожок - чебурек')

'''Обработка total score и выгрузка'''
score_total_list = []
for i in assignee_id:
    if len(df_status.loc[(df_status.assignee_id==i) & (df_status.status=='closed')].score_value.values) != 0:
        closed = df_status.loc[(df_status.assignee_id==i) & (df_status.status=='closed')].score_value.values[0]
    else:
        closed = 0
    if len(df_status.loc[(df_status.assignee_id==i) & (df_status.status=='solved')].score_value.values) != 0:
        solved = df_status.loc[(df_status.assignee_id==i) & (df_status.status=='solved')].score_value.values[0]
    else:
        solved = 0
    score_total = np.mean([closed, solved])
    score_total_list.append(score_total)

df_out = pd.DataFrame({'assignee_id': assignee_id, 'score_total': score_total_list})

insertScoreResultTotalData(df_out,'findcsystem','xsolla_summer_school','score_result_total')
print('Котенок - Тигренок')


'''Обработка загруженности по статусам и каналам и выгрузка'''
df_status_channel = pd.DataFrame()
for i in assignee_id:
    df = workloadScoringByStatusesChannel(DataFrame[DataFrame.assignee_id == i][:],63,7)
    df1 = pd.concat([df_status_channel,df])
df_status_channel = df_status_channel.reset_index(drop=True)

insertScoreResultDataChannel(df1,'findcsystem','xsolla_summer_school','score_result_status_channel')
print('Яблоко - автомобиль')