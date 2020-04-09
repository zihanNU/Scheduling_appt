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
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'median',
              'Dwell': 'median'})\
        .rename(columns={'histloadID': 'count'})
    agg_df_sort = agg_df.sort_values(by=['PU_FacilityID', 'DO_FacilityID', 'LoadID', 'count', 'similarity', 'Dwell'],
                                     ascending=[True, True, True, False, False, True]).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    hour_df = agg_df_sort.loc[df_rank <= 3]
    return hour_df


def schedule_hour_area(df):
    agg_df = df.groupby(['LoadID', 'OriginCluster', 'DestCluster', 'PU_Bucket', 'DO_Bucket'], as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'median',
              'Dwell': 'median'}) \
        .rename(columns={'histloadID': 'count'})
    agg_df_sort = agg_df.sort_values(by=['LoadID', 'count', 'similarity', 'Dwell'],
                                     ascending=[True, False, False, True]).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    hour_df = agg_df_sort.loc[df_rank <= 3]
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


def scheduler_ml_AD(newloads_df, histloads_df):
    df_dict = similarity_check(newloads_df, histloads_df)
    facility_hour_all_df = df_dict['facility_hour_all_df']
    facility_dwell_df = df_dict['facility_dwell_df']
    facility_travel_df = df_dict['facility_travel_df']
    facility_area_df = df_dict['facility_area_df']
    facility_hour_TypeA = schedule_hour_stop(facility_hour_all_df)
    facility_hour_TypeD = schedule_hour_area(facility_area_df)
    dwell_df = cal_dwell(facility_dwell_df)
    transit_df = cal_transit(facility_travel_df)
    loadid_all = newloads_df['LoadID'].tolist()
    loadid_partTypeA = facility_hour_TypeA['LoadID'].tolist()
    loadid_partTypeD = facility_hour_TypeD['LoadID'].tolist()
    typeD_id = list(set(loadid_partTypeD) - set(loadid_partTypeA))
    typeE_id = list(set(loadid_all) - set(loadid_partTypeD) - set(loadid_partTypeA))
    rename_dict = {'PU_Facility': 'PU_FacilityID', 'DO_Facility': 'DO_FacilityID'}
    facility_hour_TypeD = facility_hour_TypeD[facility_hour_TypeD['LoadID'].isin(typeD_id)]
    facility_hour_TypeD = facility_hour_TypeD.merge(newloads_df[['LoadID', 'PU_Facility', 'DO_Facility']],
                                                    on=['LoadID'], how='left', copy=False).rename(columns=rename_dict)
    features = ['LoadID', 'PU_FacilityID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket', 'count', 'similarity', 'PU_Hour',
                'DO_Hour',
                'Transit', 'Dwell']
    newloads_ml = pd.concat(facility_hour_TypeA[features], facility_hour_TypeD[features], axis=0, ignore_index=True)
    if len(typeE_id) > 0:
        load_typeE = newloads_df[newloads_df['LoadID'].isin(typeE_id)]
        facility_hour_TypeB = scheduler_newFac(load_typeE, dwell_df, transit_df)
    return newloads_ml, facility_hour_TypeB


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

    Type E: no data for facility nor cluster.
    :param newloads_df1: both need appts
    :param newloads_df1: only one need appts
    :param histloads_df:
    :return:
    '''
    newloads_part1_ind = (newloads_df['PU_Appt'].isna()) & (newloads_df['DO_Appt'].isna())
    newloads_part2_ind = (newloads_df['PU_Appt'].isna()) & (newloads_df['DO_ScheduleType'].values > 1)
    newloads_part3_ind = (newloads_df['PU_ScheduleType'].values > 1) & (newloads_df['DO_Appt'].isna())
    newloads_part4_ind = ~(newloads_part1_ind | newloads_part2_ind | newloads_part3_ind)
    #note the transit time and dwell time have been set into hours
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
        newload_scheduler_part1, newload_typeB_part1 = scheduler_ml_AD(newloads_part1, histloads_df)

    if newloads_part2_ind.any():
        #PU need appt, but DO is free for appt. can use type A or D
        newloads_part2 = newloads_df[newloads_part2_ind].reset_index(drop=True)
        newload_scheduler_part2, newload_typeB_part2 = scheduler_ml_AD(newloads_part2, histloads_df)

    if newloads_part3_ind.any():
        #DO need appt, but PU is free for appt. can use type A
        newloads_part3 = newloads_df[newloads_part3_ind].reset_index(drop=True)
        newload_scheduler_part3, newload_typeB_part3 = scheduler_ml_AD(newloads_part3, histloads_df)




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

def scheduler_newFac(newload_df, facility_hour_df):  #Type E
    speed_base = 45
    newload_df['traveltime'] = newload_df['Miles'].values / speed_base
    newload_df['resttime'] = np.int32(newload_df['traveltime'].values / 10) * 10 + np.int32(
        newload_df['traveltime'].values / 4)
    newload_df['transit'] = newload_df['traveltime'] + newload_df['resttime']
    newload_df['dwelltime'] = 2
    ## logic here:
    # we have 3 types: both need appts, but no data, in this type, we can always set pu appt, and calculate do appt
    # second, pu need appt, and do do not need appt, in this type, we can still set pu appt first and ignore do appt or set whatever works
    # third, do need appt, and pu do not need appt, here it is tricky to check facility time
    buffer = 1

    newload_df['pu_scheduletime'] = pd.to_datetime(newload_df['LoadDate']) + pd.to_timedelta(10, unit='h')
    newload_df['do_scheduletime'] = pd.to_datetime(newload_df['pu_scheduletime']) + \
                                    pd.to_timedelta((newload_df['transit'].values + newload_df['dwelltime'].values
                                                     - newload_df['PUOffset'].values + newload_df['DOOffset'].values
                                                     + buffer), unit='h')
    features = ['LoadID', 'LoadDate', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_ScheduleType', 'DO_Appt', 'do_scheduletime','PU_DOW']

    newloads_result_df = feasibility_check(newload_df[features], facility_hour_df)

    return newloads_result_df.reset_index(drop=True)


def scheduler_rule(newload_df, dwell_df, transit_df, facility_hour_df):  #Type B and C
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
                                                       - do_newloaddf['PUOffset'].values + do_newloaddf['DOOffset'].values
                                                       + buffer), unit='h')
    pu_newloaddf['pu_scheduletime'] = pd.to_datetime(pu_newloaddf['DO_Appt']).values - \
                                      pd.to_timedelta((pu_newloaddf['transit'].values + pu_newloaddf['dwelltime'].values
                                                      - pu_newloaddf['PUOffset'].values + pu_newloaddf['DOOffset'].values
                                                      + buffer), unit='h')
    features = ['LoadID', 'LoadDate','PU_Facility', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime', 'PU_DOW']

    result_df = pd.concat(pu_newloaddf[features], do_newloaddf[features], axis=0, ignore_index=True)
    newloads_result_df = feasibility_check(result_df, facility_hour_df)
    return newloads_result_df

def feasibility_check(load_df, facility_df):
    Weekday_Mapper = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    load_df['DO_DOW'] = -1
    do_ind = load_df['DO_ScheduleType'].values == 1
    pu_ind = load_df['PU_ScheduleType'].values == 1
    load_df.loc[do_ind, 'DO_DOW'] = load_df['do_scheduletime'].dt.dayofweek


