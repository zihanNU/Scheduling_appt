import pandas as pd
import numpy as np
import config
from .similarity_functions import similarity_check
import logging

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)


def join_All_agg(facility_hour_ml_df):
    facility_hour_ml_df = pd.DataFrame(facility_hour_ml_df)
    hour_ml_agg_df = facility_hour_ml_df.groupby(['LoadID', 'PU_FacilityID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket'],
                                                 as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'mean',
              'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'})
    hour_ml_agg_df = hour_ml_agg_df.loc[hour_ml_agg_df['count']>=10]
    hour_ml_agg_df_sort = hour_ml_agg_df.sort_values(by=['LoadID', 'count', 'similarity'], ascending=False).reset_index(drop=True)
    df_rank = hour_ml_agg_df_sort.groupby(['LoadID']).cumcount()
    hour_ml_agg_df_select = hour_ml_agg_df[df_rank <= 3]
    return hour_ml_agg_df_select


def scheduler_model(newloads_df1, newloads_df2, histloads_df):
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


    :param newloads_df:
    :param histloads_df:
    :return:
    '''
    df_dict = similarity_check(newloads_df, histloads_df)
    facility_hour_all_df = df_dict['facility_hour_all_df']
    facility_dwell_df = df_dict['facility_dwell_df']
    facility_travel_df = df_dict['facility_travel_df']
    facility_area_df = df_dict['facility_area_df']
    ## Statistics based --ML Similarity


    facility_hour_A = join_All_agg(similarity_cal_all(df))



def scheduler_ml(newloads_df, histloads_df): #Type A and D

    hour_ml_df = similarity_join_score_np(facility_hour_ml_df, model_flag=0)


def scheduler_rule(newloads_df, histloads_df):  #Type B and C

