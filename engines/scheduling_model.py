import pandas as pd
import numpy as np
from numpy.linalg import norm
import config
import logging

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)





def similarity_join_score_np(facility_hour_ml_df, model_flag):
    ###model_flag: 0 for both, 1 for pu -1 for do.
    facility_hour_ml_df=facility_hour_ml_df.sort_values(by=['LoadID', 'similarity'], ascending=False)
    if model_flag >= 0:
        hour_ml = facility_hour_ml_df.groupby(['LoadID'], as_index=False)[['PU_Hour_ml']].median()
        #hour_ml['PU_Hour_ml']=hour_ml['PU_Hour_ml']/hour_ml['similarity']
    if model_flag <= 0:
        hour_ml = facility_hour_ml_df.groupby(['LoadID'], as_index=False)[['similarity', 'DO_Hour_ml']].sum()
        hour_ml['DO_Hour_ml'] = hour_ml['DO_Hour_ml'] / hour_ml['similarity']
    hour_ml.columns = ['LoadID', 'PU_Hour_ml', 'DO_Hour_ml']
    return hour_ml

def scheduler_ml(newloads_df,histloads_df):
    newloadfeatures = ['LoadID', 'Miles', 'TotalWeight', 'PU_Facility', 'CustomerID', 'DO_Facility']
    newhistjoin_df1 = newloads_df[newloadfeatures]
    newhistjoin_df1.columns = ['LoadID', 'new_miles', 'new_weight', 'new_PF', 'new_customer', 'new_DF']
    histloadfeatures = ['LoadID', 'PU_Hour', 'DO_Hour', 'PU_Transit_Minute', 'Miles', 'TotalWeight',
                        'PU_FacilityID', 'CustomerID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket']
    newhistjoin_df2 = histloads_df[histloadfeatures]
    newhistjoin_df2.columns = ['histloadID', 'PU_Hour', 'DO_Hour', 'PU_Transit_Minute', 'hist_miles',
                               'hist_weight', 'PU_Facility', 'hist_customer',
                               'DO_Facility', 'PU_Bucket', 'DO_Bucket']
    hour_ml_df = pd.DataFrame(columns=['LoadID', 'PU_Hour_ml', 'DO_Hour_ml'])
    try:
        facilityjoin_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=["new_PF", "new_DF"],
                                                right_on=["PU_Facility", "DO_Facility"], how='inner')
        facilityjoin_df.drop(['new_PF', 'new_DF'], axis=1, inplace=True)

    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility PU and DO: {0}'.format(e))
    if facilityjoin_df.shape[0] > 0:
        facility_hour_ml_df = similarity_cal_ml(facilityjoin_df)
        hour_ml_df.append(similarity_join_score_np(facility_hour_ml_df, model_flag=0))


    try:
        destjoin_df = newhistjoin_df1.merge(newhistjoin_df2, left_on="new_DF",
                                            right_on="hist_DF", how='inner')
    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility DO: {0}'.format(e))
    destjoin_df = destjoin_df.loc[destjoin_df.new_PF!=destjoin_df.hist_PF]
    if destjoin_df.shape[0] > 0:
        facility_hour_ml_df = similarity_cal_ml(destjoin_df)
        hour_ml_df.append(similarity_join_score_np(facility_hour_ml_df, model_flag=-1))


    try:
        originjoin_df = newhistjoin_df1.merge(newhistjoin_df2, left_on="new_PF", right_on="hist_PF",  how='inner')
    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility PU: {0}'.format(e))
    originjoin_df = originjoin_df.loc[originjoin_df.new_DF!=originjoin_df.hist_DF]
    if originjoin_df.shape[0] > 0:
        facility_hour_ml_df = similarity_cal_ml(originjoin_df)
        hour_ml_df.append(similarity_join_score_np(facility_hour_ml_df, model_flag=1))

    return hour_ml_df


def scheduler_rule(newloads_df, histloads_df):
    newloadfeatures = ['LoadID', 'Miles', 'PU_Facility', 'CustomerID', 'DO_Facility', 'TotalWeight',
                      'OriginLatitude', 'OriginLongitude', 'DestinationLatitude', 'DestinationLongitude',
                      ]
    histloadfeatures = ['LoadID', 'PU_Hour', 'DO_Hour', 'Miles', 'PU_Facility', 'CustomerID', 'DO_Facility',
                        'PU_Transit_Minute', 'PU_Dwell_Minute', 'TotalWeight',
                       'OriginLatitude', 'OriginLongitude', 'DestinationLatitude', 'DestinationLongitude',
                       ]
    newloads_df_pu = newloads_df.loc[newloads_df['PU_Appt'].isna()]
    newloads_df_do = newloads_df.loc[newloads_df['PU_Appt'].isna()]



    hour_ml_df = pd.DataFrame(columns=['LoadID', 'PU_Hour_ml', 'DO_Hour_ml'])
    try:
        facilityjoin_df = newhistjoin_df1.merge(newhistjoin_df2, on=["PU_Facility", "DO_Facility"],
                                                suffixes=('_new', '_hist'), how='inner')
    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility PU and DO: {0}'.format(e))

    try:
        originjoin_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=["PU_Facility"],
                                              suffixes=('_new', '_hist'), how='inner')
    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility PU: {0}'.format(e))
    try:
        destjoin_df = newhistjoin_df1.merge(newhistjoin_df2, on=["DO_Facility"],
                                            suffixes=('_new', '_hist'), how='inner')
    except Exception as e:
        LOGGER.exception('unexpected error while merging on Facility DO: {0}'.format(e))
