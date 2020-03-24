import logging

import pandas as pd
import numpy as np
from numpy.linalg import norm
import config
from .distance_function import approx_dist


CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)


def similarity_check(newloads_df, histloads_df):
    newloadfeatures = ['LoadID', 'Miles', 'TotalWeight', 'PU_Facility', 'CustomerID', 'DO_Facility',
                       'OriginClusterID', 'DestinationClusterID',
                       'OriginLatitude', 'OriginLongitude', 'DestinationLatitude', 'DestinationLongitude']
    newhistjoin_df1 = newloads_df[newloadfeatures]
    newhistjoin_df1.columns = ['LoadID', 'new_miles', 'new_weight', 'new_PF', 'new_customer', 'new_DF',
                               'new_Orig_Cluster', 'new_Dest_Cluster',
                               'new_originLat','new_originLon', 'new_destinationLat', 'new_destinationLon',
                               ]
    histloadfeatures = ['LoadID', 'PU_Hour', 'DO_Hour', 'PU_Transit_Minute', 'Miles', 'TotalWeight',
                        'PU_FacilityID', 'CustomerID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket',
                        'OriginClusterID', 'DestinationClusterID',
                        'OriginLatitude', 'OriginLongitude', 'DestinationLatitude', 'DestinationLongitude']
    newhistjoin_df2 = histloads_df[histloadfeatures]
    newhistjoin_df2.columns = ['histloadID', 'PU_Hour', 'DO_Hour', 'PU_Transit_Minute', 'hist_miles',
                               'hist_weight', 'PU_Facility', 'hist_customer',
                               'DO_Facility', 'PU_Bucket', 'DO_Bucket',
                               'hist_Orig_Cluster', 'hist_Dest_Cluster',
                               'hist_originLat', 'hist_originLon', 'hist_destinationLat', 'hist_destinationLon',
                               ]
    joinAll_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=["new_PF", "new_DF"],
                                       right_on=["PU_Facility", "DO_Facility"], how='inner')
    joinAll_df.drop(['new_PF', 'new_DF', 'new_Orig_Cluster', 'new_Dest_Cluster',  'hist_Orig_Cluster','hist_Dest_Cluster',
                     'new_originLat', 'new_originLon', 'new_destinationLat', 'new_destinationLon',
                     'hist_originLat', 'hist_originLon', 'hist_destinationLat', 'hist_destinationLon',
                     ], axis=1, inplace=True)

    joinOrig_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=["new_PF"], right_on=["PU_Facility"], how='inner')
    joinOrig_df.drop(['new_PF', 'new_Orig_Cluster', 'new_Dest_Cluster',  'hist_Orig_Cluster', 'hist_Dest_Cluster'],
                     axis=1, inplace=True)

    joinDest_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=["new_DF"], right_on=["DO_Facility"], how='inner')
    joinDest_df.drop(['new_PF', 'new_Orig_Cluster', 'new_Dest_Cluster', 'hist_Orig_Cluster', 'hist_Dest_Cluster'],
                     axis=1, inplace=True)

    joinArea_df = newhistjoin_df1.merge(newhistjoin_df2, left_on=['new_Orig_Cluster', 'new_Dest_Cluster'],
                                        right_on=['hist_Orig_Cluster', 'hist_Dest_Cluster'], how='inner')
    joinArea_df.drop(['new_PF'], axis=1, inplace=True)


    if joinAll_df.shape[0] > 0:  # Type A
        facility_hour_all_df = similarity_cal_all(joinAll_df)

    if joinOrig_df.shape[0] > 0:
        facility_dwell_df = similarity_cal_dwell(joinOrig_df)

    # if joinDest_df.shape[0] > 0:
    #     facility_dwell_df = similarity_cal_dwell(joinDest_df)

    if joinArea_df.shape[0] > 0:
        facility_travel_df = similarity_cal_travel(joinArea_df)

        facility_area_df = similarity_cal_area(joinArea_df)

    dict_all = {'facility_hour_all_df': facility_hour_all_df,
                'facility_dwell_df': facility_dwell_df,
                'facility_travel_df': facility_travel_df,
                'facility_area_df': facility_area_df}

    return dict_all


def similarity_join_np(facility_hour_ml_df, model_flag):
    ###model_flag: 0 for both, 1 for pu -1 for do.
    facility_hour_ml_df = facility_hour_ml_df.sort_values(by=['LoadID', 'similarity'], ascending=False)
    if model_flag >= 0:
        hour_ml = facility_hour_ml_df.groupby(['LoadID'], as_index=False)[['PU_Hour_ml']].median()
        #hour_ml['PU_Hour_ml']=hour_ml['PU_Hour_ml']/hour_ml['similarity']
    if model_flag <= 0:
        hour_ml = facility_hour_ml_df.groupby(['LoadID'], as_index=False)[['similarity', 'DO_Hour_ml']].sum()
        hour_ml['DO_Hour_ml'] = hour_ml['DO_Hour_ml'] / hour_ml['similarity']
    hour_ml.columns = ['LoadID', 'PU_Hour_ml', 'DO_Hour_ml']
    return hour_ml


def similarity_cal_all(df):
    histload_feature = {'weight': df['hist_weight']/CONFIG.WEIGHTSCALE,
                        'length': df['hist_miles']/CONFIG.MILESCALE,
                        'pallet': df['hist_pallet'] / CONFIG.WEIGHTSCALE,
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'weight': df['new_weight']/CONFIG.WEIGHTSCALE,
                       'length': df['new_miles']/CONFIG.MILESCALE,
                       'pallet': df['new_pallet'] / CONFIG.WEIGHTSCALE,
                       }
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    sim_df = {'LoadID': df['LoadID'],
              'histloadID': df['histloadID'],
              'similarity': sim_row,
              'PU_FacilityID': df['PUFacilityID'],
              'PU_Hour': df['PU_Hour'],
              'PU_Bucket': df['PUBucket'],
              'DO_FacilityID': df['DOFacilityID'],
              'DO_Hour': df['DO_Hour'],
              'DO_Bucket': df['DOBucket'],
              'Transit': df['PU_Transit_Minute'],
              'Dwell': df['PU_Dwell_Minute']
              }
    return sim_df


def similarity_cal_dwell(df):
    histload_feature = {'weight': df['hist_weight']/CONFIG.WEIGHTSCALE,
                        'length': df['hist_miles']/CONFIG.MILESCALE,
                        'pallet': df['hist_pallet'] / CONFIG.WEIGHTSCALE,
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'weight': df['new_weight']/CONFIG.WEIGHTSCALE,
                       'length': df['new_miles']/CONFIG.MILESCALE,
                       'pallet': df['new_pallet'] / CONFIG.WEIGHTSCALE,
                       }
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    sim_df = {'LoadID': df['LoadID'],
                           'histloadID': df['histloadID'],
                           'similarity': sim_row,
                           'PU_FacilityID': df['PUFacilityID'],
                           'PU_Hour': df['PU_Hour'],
                           'PU_Bucket': df['PUBucket'],
                           'Dwell': df['PU_Dwell_Minute']
                    }
    return sim_df


def similarity_cal_travel(df):
    df['oridist'] = approx_dist(
        origin_latitude=df['new_originLat'].values,
        origin_longitude=df['new_originLon'].values,
        dest_latitude=df['hist_originLat'].values,
        dest_longitude=df['hist_originLon'].values
    ) / CONFIG.OD_DIST_SCALE
    df['destdist'] = approx_dist(
        origin_latitude=df['new_destinationLat'].values,
        origin_longitude=df['new_destinationLon'].values,
        dest_latitude=df['hist_destinationLat'].values,
        dest_longitude=df['hist_destinationLon'].values
    ) / CONFIG.OD_DIST_SCALE

    df['ref'] = CONFIG.SIMILARITY_REF

    histload_feature = {'oridist': df['oridist'],
                        'destdist': df['destdist'],
                        'weight': df['hist_weight'] / CONFIG.WEIGHTSCALE,
                        'length': df['hist_miles'] / CONFIG.MILESCALE,
                        'pallet': df['hist_pallet'] / CONFIG.WEIGHTSCALE,
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'oridist': df['ref'],
                       'destdist': df['ref'],
                       'weight': df['new_weight'] / CONFIG.WEIGHTSCALE,
                       'length': df['new_miles'] / CONFIG.MILESCALE,
                       'pallet': df['new_pallet'] / CONFIG.WEIGHTSCALE,
                       }
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    sim_df = {'LoadID': df['LoadID'],
              'histloadID': df['histloadID'],
              'similarity': sim_row,
              'OriginCluster': df['new_OrigClusterID'],
              'DestCluster': df['new_DestClusterID'],
              'Transit': df['PU_Transit_Minute']
              }
    return sim_df

def similarity_cal_area(df):
    df['oridist'] = approx_dist(
        origin_latitude=df['new_originLat'].values,
        origin_longitude=df['new_originLon'].values,
        dest_latitude=df['hist_originLat'].values,
        dest_longitude=df['hist_originLon'].values
    ) / CONFIG.OD_DIST_SCALE
    df['ref'] = CONFIG.SIMILARITY_REF

    histload_feature = {'oridist': df['oridist'],
                        'weight': df['hist_weight'] / CONFIG.WEIGHTSCALE,
                        'length': df['hist_miles'] / CONFIG.MILESCALE,
                        'pallet': df['hist_pallet'] / CONFIG.WEIGHTSCALE,
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'oridist': df['ref'],
                       'weight': df['new_weight'] / CONFIG.WEIGHTSCALE,
                       'length': df['new_miles'] / CONFIG.MILESCALE,
                       'pallet': df['new_pallet'] / CONFIG.WEIGHTSCALE,
                       }
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    sim_df = {'LoadID': df['LoadID'],
              'histloadID': df['histloadID'],
              'similarity': sim_row,
              'OriginCluster': df['new_OrigClusterID'],
              'DestCluster': df['new_DestClusterID'],
              'PU_Hour': df['PU_Hour'],
              'PU_Bucket': df['PU_Bucket'],
              'DO_Hour': df['DO_Hour'],
              'DO_Bucket': df['DO_Bucket'],
              }
    return sim_df