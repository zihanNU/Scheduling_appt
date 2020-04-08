import pandas as pd
import numpy as np
import config
from .similarity_functions import similarity_check
import logging
from datetime import timedelta
#from collections import Counter

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)


def schedule_hour_stop(df):
    agg_df = df.groupby(['LoadID', 'PU_FacilityID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket'], as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'mean',
              'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    select_agg_df = agg_df.loc[agg_df['count']>=10]
    agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'similarity'], ascending=False).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    hour_df = select_agg_df[df_rank <= 3]
    return hour_df


def schedule_hour_area(df):
    agg_df = df.groupby(['LoadID', 'OriginCluster', 'DestCluster', 'PU_Bucket', 'DO_Bucket'], as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'mean',
              'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    select_agg_df = agg_df.loc[agg_df['count'] >= 10]
    agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'similarity'],
                                            ascending=False).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    hour_df = select_agg_df[df_rank <= 3]
    return hour_df


def cal_dwell(df):
    df['weightDwell'] = df['similarity'].values * df['Dwell'].values
    agg_df = df.groupby(['LoadID', 'PU_FacilityID', 'PU_Bucket'], as_index=False) \
        .agg({'histloadID': 'size', 'similarity': ['median', 'sum'], 'PU_Hour': 'mean', 'Dwell': 'mean', 'weightDwell': 'sum'})

    agg_df.columns = ['LoadID', 'PU_FacilityID', 'PU_Bucket', 'count', 'sim_median', 'sim_sum', 'PU_Hour', 'Dwell', 'weightDwell']
    select_agg_df = agg_df.loc[(agg_df['count'] >= 3) & (agg_df['Dwell'].values <= 600) & (agg_df['Dwell'].values > 0)]
    select_agg_df['Dwell_est'] = np.where(select_agg_df['sim_median'].values > 0.9, select_agg_df['Dwell'].values,
                                          select_agg_df['weightDwell'].values / select_agg_df['sim_sum'].values)
    # agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'sim_median'], ascending=False).reset_index(drop=True)
    # df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    # dwell_df = agg_df_sort[df_rank <= 3]
    return select_agg_df


def cal_transit(df):
    df['weightTransit'] = df['similarity'].values * df['Transit'].values
    agg_df = df.groupby(['LoadID', 'OriginCluster', 'DestCluster'], as_index=False)\
        .agg({'histloadID': 'size', 'similarity': ['median', 'sum'], 'Transit': 'mean', 'weightTransit': 'sum'})
    agg_df.columns = ['LoadID', 'OriginCluster', 'DestCluster', 'count', 'sim_median', 'sim_sum', 'Transit', 'weightTransit']
    select_agg_df = agg_df.loc[(agg_df['count'] >= 3) & (agg_df['Transit'] >= 0)]
    select_agg_df['Travel_est'] = np.where(select_agg_df['sim_median'].values > 0.9, select_agg_df['Transit'].values,
                                           select_agg_df['weightTransit'].values / select_agg_df['sim_sum'].values)
    # agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'sim_median'], ascending=False).reset_index(drop=True)
    # df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    # transit_df = select_agg_df[df_rank <= 3]
    return select_agg_df



def scheduler_model(newloads_df, histloads_df):
    '''
    Defines four type of models:
    Type A: both facility have enough hist loads to check, and both appt needs appointment.
    We can set them together through similarity check and historical data for both facility

    Type B:
    only one side needs Appt, needs to check dwell time for PU and transit time between O and D
    --Need to call Dwell and Travel similarity to calculate

    Type C:
    both facility need Appt, but only one side have enough data to check

    Type D: both facility need Appt, but both side do not have enough data.


    :param newloads_df1: both need appts
    :param newloads_df1: only one need appts
    :param histloads_df:
    :return:
    '''
    newloads_part1_ind = (newloads_df['PU_Appt'].isna()) & (newloads_df['DO_Appt'].isna())
    newloads_part2_ind = (newloads_df['PU_Appt'].isna()) & (newloads_df['DO_ScheduleType'].values > 1)
    newloads_part3_ind = (newloads_df['PU_ScheduleType'].values > 1) & (newloads_df['DO_Appt'].isna())
    newloads_part4_ind = ~(newloads_part1_ind | newloads_part2_ind | newloads_part3_ind)

    # only one side need appt, and the other side is fixed
    if newloads_part4_ind.all():
        newload_part4 = newloads_df[newloads_part4_ind].reset_index(drop=True)
        df_dict = similarity_check(newload_part4, histloads_df)
        facility_dwell_df = df_dict['facility_dwell_df']
        facility_travel_df = df_dict['facility_travel_df']
        dwell_df = cal_dwell(facility_dwell_df)
        transit_df = cal_transit(facility_travel_df)
        facility_hour_TypeC = scheduler_rule(newload_part4, dwell_df, transit_df)

    # no side is fixed
    if newloads_part1_ind.any():
        # both need appt
        newloads_part1 = newloads_df[newloads_part1_ind].reset_index(drop=True)
        df_dict = similarity_check(newloads_part1, histloads_df)
        facility_hour_all_df = df_dict['facility_hour_all_df']
        facility_dwell_df = df_dict['facility_dwell_df']
        facility_travel_df = df_dict['facility_travel_df']
        facility_area_df = df_dict['facility_area_df']
        facility_hour_TypeA = schedule_hour_stop(facility_hour_all_df)
        facility_hour_TypeD = schedule_hour_area(facility_area_df)
        dwell_df = cal_dwell(facility_dwell_df)
        transit_df = cal_transit(facility_travel_df)
        loadid_all = newloads_part1_ind['LoadID'].tolist()
        loadid_partTypeA = facility_hour_TypeA['LoadID'].tolist()
        loadid_partTypeD = facility_hour_TypeD['LoadID'].tolist()
        typeD_id = list(set(loadid_partTypeD) - set(loadid_partTypeA))
        typeB_id = list(set(loadid_all) - set(loadid_partTypeD) - set(loadid_partTypeA))
        facility_hour_TypeD = facility_hour_TypeD[facility_hour_TypeD['LoadID'].isin(typeD_id)]
        if len(typeB_id) > 0:
            load_typeB = newloads_df[newloads_df['LoadID'].isin(typeB_id)]
            facility_hour_TypeB = scheduler_rule(load_typeB, dwell_df, transit_df)

    if newloads_part2_ind.any():
        #PU need appt, but DO is free for appt. can use type A or D
        newloads_part2 = newloads_df[newloads_part2_ind].reset_index(drop=True)
        df_dict = similarity_check(newloads_part2, histloads_df)
        facility_hour_all_df = df_dict['facility_hour_all_df']
        facility_dwell_df = df_dict['facility_dwell_df']
        facility_travel_df = df_dict['facility_travel_df']
        facility_area_df = df_dict['facility_area_df']
        facility_hour_TypeA = schedule_hour_stop(facility_hour_all_df)
        facility_hour_TypeD = schedule_hour_area(facility_area_df)
        dwell_df = cal_dwell(facility_dwell_df)
        transit_df = cal_transit(facility_travel_df)
        loadid_all = newloads_part1_ind['LoadID'].tolist()
        loadid_partTypeA = facility_hour_TypeA['LoadID'].tolist()
        loadid_partTypeD = facility_hour_TypeD['LoadID'].tolist()
        typeD_id = list(set(loadid_partTypeD) - set(loadid_partTypeA))
        typeB_id = list(set(loadid_all) - set(loadid_partTypeD) - set(loadid_partTypeA))
        facility_hour_TypeD = facility_hour_TypeD[facility_hour_TypeD['LoadID'].isin(typeD_id)]
        if len(typeB_id) > 0:
            load_typeB = newloads_df[newloads_df['LoadID'].isin(typeB_id)]
            facility_hour_TypeB = scheduler_rule(load_typeB, dwell_df, transit_df)

    if newloads_part3_ind.any():
        #DO need appt, but PU is free for appt. can use type A
        newloads_part3 = newloads_df[newloads_part3_ind].reset_index(drop=True)
        df_dict = similarity_check(newloads_part3, histloads_df)
        facility_hour_all_df = df_dict['facility_hour_all_df']
        facility_dwell_df = df_dict['facility_dwell_df']
        facility_travel_df = df_dict['facility_travel_df']
        facility_area_df = df_dict['facility_area_df']
        facility_hour_TypeA = schedule_hour_stop(facility_hour_all_df)
        facility_hour_TypeD = schedule_hour_area(facility_area_df)
        dwell_df = cal_dwell(facility_dwell_df)
        transit_df = cal_transit(facility_travel_df)
        loadid_all = newloads_part1_ind['LoadID'].tolist()
        loadid_partTypeA = facility_hour_TypeA['LoadID'].tolist()
        loadid_partTypeD = facility_hour_TypeD['LoadID'].tolist()
        typeD_id = list(set(loadid_partTypeD) - set(loadid_partTypeA))
        typeB_id = list(set(loadid_all) - set(loadid_partTypeD) - set(loadid_partTypeA))
        facility_hour_TypeD = facility_hour_TypeD[facility_hour_TypeD['LoadID'].isin(typeD_id)]
        if len(typeB_id) > 0:
            load_typeB = newloads_df[newloads_df['LoadID'].isin(typeB_id)]
            facility_hour_TypeB = scheduler_rule(load_typeB, dwell_df, transit_df)




def transit_time(miles, speed_base=45):
    traveltime = miles/speed_base
    resttime = np.int(traveltime/10) * 10 + np.int32(traveltime / 4)
    return traveltime + resttime
    # if traveltime <= 10:
    #     resttime = np.int32(traveltime / 4)
    # elif traveltime <= 20:
    #     resttime = 10 + np.int32(traveltime / 4)
    # elif traveltime <= 30:
    #     resttime = 10*2 + np.int32(traveltime / 4)
    # elif traveltime <= 40:
    #     resttime = 10*3 + np.int32(traveltime / 4)
    # elif traveltime <= 50:
    #     resttime = 10*4 + np.int32(traveltime / 4)
    # elif traveltime <= 60:
    #     resttime = 10*5 + np.int32(traveltime / 4)
    # elif traveltime <= 70:
    #     resttime = 10*6 + np.int32(traveltime / 4)
    # elif traveltime <= 80:
    #     resttime = 10*7 + np.int32(traveltime / 4)




def scheduler_rule(newload_df, dwell_df, transit_df):  #Type B and C
    speed_base = 45
    newload_df['traveltime'] = newload_df['Miles'].values / speed_base
    newload_df['resttime'] = np.int32(newload_df['traveltime'].values / 10) * 10 + np.int32(
        newload_df['traveltime'].values / 4)
    newload_df['transit'] = newload_df['traveltime'] + newload_df['resttime']
    newload_df['dwelltime'] = 2
    pu_ind = newload_df['PU_Appt'].isna()

    hour_bucket = [0, 5, 8, 11, 14, 18, 21, 25]

    pu_newloaddf = newload_df.loc[pu_ind].reset_index(drop=True)
    do_newloaddf = newload_df.loc[~pu_ind].reset_index(drop=True)
    do_newloaddf['PU_Hour'] = pd.to_datetime(do_newloaddf['PU_Appt']).dt.hour
    do_newloaddf['PU_Bucket'] = pd.cut(do_newloaddf['PU_Hour'], bins=hour_bucket).cat.codes

    do_newloaddf = do_newloaddf.merge(dwell_df[['LoadID', 'PU_Bucket', 'Dwell_est']],
                                                on=['LoadID', 'PU_Bucket'], how='left')
    do_newloaddf['Dwell_est'].fillna(2, inplace=True)
    do_newloaddf['dwelltime'] = do_newloaddf['Dwell_est']

    pu_newloaddf['pu_scheduletime'] = pd.NaT
    do_newloaddf['do_scheduletime'] = pd.NaT
    buffer = 1 #1 hour

    do_newloaddf['do_scheduletime'] = pd.to_datetime(do_newloaddf['PU_Appt']).values + \
                                      pd.to_timedelta((do_newloaddf['transit'].values + do_newloaddf['dwelltime'].values
                                                       + do_newloaddf['PUOffset'].values - do_newloaddf['DOOffset'].values
                                                       + buffer), unit='h')
    pu_newloaddf['pu_scheduletime'] = pd.to_datetime(pu_newloaddf['DO_Appt']).values - \
                                      pd.to_timedelta((pu_newloaddf['transit'].values + pu_newloaddf['dwelltime'].values
                                                      + pu_newloaddf['PUOffset'].values - pu_newloaddf['DOOffset'].values
                                                      + buffer), unit='h')
    features = ['LoadID', 'LoadDate', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_ScheduleType', 'DO_Appt', 'do_scheduletime']
    return pd.concat(pu_newloaddf[features], do_newloaddf[features], axis=1, ignore_index=True)

