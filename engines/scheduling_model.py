import pandas as pd
import numpy as np
import config
from .similarity_functions import similarity_check
import logging
from collections import Counter

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)


def schedule_hour_stop(df):
    agg_df = df.groupby(['LoadID', 'PU_FacilityID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket'],
                                                 as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'mean',
              'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    select_agg_df = agg_df.loc[agg_df['count']>=10]
    agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'similarity'], ascending=False).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    hour_df = select_agg_df[df_rank <= 3]
    return hour_df


def schedule_hour_area(df):
    agg_df = df.groupby(['LoadID', 'OriginCluster', 'DestCluster', 'PU_Bucket', 'DO_Bucket'],
                                                 as_index=False) \
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
    agg_df = df.groupby(['LoadID', 'PU_FacilityID', 'PU_Bucket'],
                                                 as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    select_agg_df = agg_df.loc[agg_df['count'] >= 10]
    agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'similarity'],
                                            ascending=False).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    dwell_df = select_agg_df[df_rank <= 3]
    return dwell_df


def cal_transit(df):
    agg_df = df.groupby(['LoadID', 'OriginCluster', 'DestCluster'], as_index=False)\
        .agg({'histloadID': 'size', 'similarity': 'median','Transit': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    select_agg_df = agg_df.loc[agg_df['count']>=10]
    agg_df_sort = select_agg_df.sort_values(by=['LoadID', 'count', 'similarity'], ascending=False).reset_index(drop=True)
    df_rank = agg_df_sort.groupby(['LoadID']).cumcount()
    transit_df = select_agg_df[df_rank <= 3]
    return transit_df



def scheduler_model(newloads_df, histloads_df, newloads_part1_ind):
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

    if newloads_part1_ind.any:
        newloads_part1 = newloads_df[newloads_part1_ind]
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
            facility_hour_TypeB = scheduler_rule (load_typeB, dwell_df, transit_df)

    if not newloads_part1_ind.any:
        newload_part2 = newloads_df[~newloads_part1_ind]
        df_dict = similarity_check(newload_part2, histloads_df)
        facility_dwell_df = df_dict['facility_dwell_df']
        facility_travel_df = df_dict['facility_travel_df']
        dwell_df = cal_dwell(facility_dwell_df)
        transit_df = cal_transit(facility_travel_df)
        facility_hour_TypeC = scheduler_rule(newload_part2, dwell_df, transit_df)


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
    pu_ind = newload_df['PU_Appt'].isna()
    pu_newloaddf = newload_df.loc[pu_ind]
    do_newloaddf = newload_df.loc[~pu_ind]
    newload_df['traveltime'] = newload_df['Miles'].values / speed_base
    newload_df['resttime'] = np.int(newload_df['traveltime'].values/10) * 10 + np.int32(newload_df['traveltime'].values/4)
    newload_df['transit'] = newload_df['traveltime'] + newload_df['resttime']
    newload_df['pu_scheduletime'] = pd.NaT
    newload_df['do_scheduletime'] = pd.NaT
    newload_df.loc[do_newloaddf, 'do_scheduletime']


