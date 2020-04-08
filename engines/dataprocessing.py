
import pandas as pd
import numpy as np
import os
import logging
import config


V_code = ['V', 'V,R', 'V,F', 'V,R,F', None]
R_code = ['R']
F_code = ['F', 'FS','F,K', 'F,SD', 'F,SD,K','SDS','SD', 'F,SD,K,FWS', 'G', 'G,DD','GS', 'K', 'V,F', 'V,R,F' ]
P_code = ['P','PO']


LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()

def assign_latlong_bycity(df,   city_df ):
    ##We need to change this function, as it is an Query related one.
    """ Query and assign lat and long based on a cityid passed in """
    try:

        name_mapper_origin = {'Latitude': 'OriginLatitude',
                              'Longitude': 'OriginLongitude',
                              'offset': 'PUOffset'}
        name_mapper_dest = {
                       'Latitude':  'DestinationLatitude',
                       'Longitude':  'DestinationLongitude',
                       'offset': 'DOOffset'}

        city_features = ['CityID', 'Latitude', 'Longitude', 'offset']
        df = pd.merge(df, city_df[city_features], left_on=['PUCityID'], right_on=["CityID"], how='left')
        df = df.rename(columns=name_mapper_origin)
        df.drop(['CityID'], axis=1, inplace=True)
        df = pd.merge(df, city_df[city_features], left_on=['DOCityID'], right_on=["CityID"], how='left')
        df.drop(['CityID'], axis=1, inplace=True)
        df = df.rename(columns=name_mapper_dest)
        return df
    except Exception as e:
        LOGGER.exception(('unexpected error variable passing: {0}'.format(e)))



def encode_load(df, columnname):
    df[columnname] = df[columnname].str.upper()
    df['V'] = df[columnname].isin(V_code)
    df['R'] = df[columnname].isin(R_code)
    df['F'] = df[columnname].isin(F_code)
    df['P'] = df[columnname].isin(P_code)
    LOGGER.info("Encoding Equipment Type for loads done")
    return df




def process_liveloads(df, city_df, cluster_df):
    LOGGER.info('Loading Live Data, Available loads...')
    #newloads_df = newloads_df[(newloads_df['Division'].isin([1, 2, 3, 4]))& (newloads_df['ShipmentType'].isin([0, 1, 2, 5]))]
    #newloads_df = newloads_df[newloads_df['TotalRate'] > 150]
    haul_bucket = [0, 200, 450, 600, 800, 1000, 1200, 1500, 2000, 2500, max(5000, df['Miles'].max())]
    df['haul'] = pd.cut(df['Miles'], bins=haul_bucket).cat.codes
    df['haul'] = df['haul']*10
    df['LoadDate'] = pd.to_datetime(df['LoadDate'], errors='coerce').dt.normalize()
    df['PU_ScheduleCloseTime'] = pd.to_datetime(df['PU_ScheduleCloseTime'], errors='coerce')
    df['DO_ScheduleCloseTime'] = pd.to_datetime(df['DO_ScheduleCloseTime'], errors='coerce')
    df['PU_LoadByDate'] = pd.to_datetime(df['PU_LoadByDate'], errors='coerce').dt.normalize()
    df['DO_LoadByDate'] = pd.to_datetime(df['DO_LoadByDate'], errors='coerce').dt.normalize()
    df['PU_time'] = pd.to_datetime(df['PU_time'], errors='coerce')
    df['DO_time'] = pd.to_datetime(df['DO_time'], errors='coerce')
    df['UpdateDate'] = pd.to_datetime(df['UpdateDate'], errors='coerce')
    df = pd.merge(df, cluster_df, left_on=["PUCityID"], right_on=["CityID"], how='left', copy=False)
    df.drop(columns=['CityID'], inplace=True)
    df = df.rename(columns={'ClusterID': 'OriginClusterID'})
    df = pd.merge(df, cluster_df, left_on=["DOCityID"], right_on=["CityID"], how='left', copy=False)
    df.drop(columns=['CityID'], inplace=True)
    df = df.rename(columns={'ClusterID': 'DestinationClusterID'})


    # only select those which need schedules
    mask_PU_ind = (df['PU_ScheduleType'].values == 1) & (df['PU_time'] <= df['LoadDate'])
    mask_DO_ind = (df['DO_ScheduleType'].values == 1) & (df['DO_time'] <= df['LoadDate'])

    schedule_df = df[mask_PU_ind | mask_DO_ind]

    schedule_df['PU_appt_nonschedule'] = schedule_df['PU_LoadByDate'].dt.normalize() + \
                                     pd.to_timedelta(schedule_df['PU_time'].dt.hour, unit='h') + \
                                     pd.to_timedelta(schedule_df['PU_time'].dt.minute, unit='m')

    schedule_df['DO_appt_nonschedule'] = schedule_df['DO_LoadByDate'].dt.normalize() + \
                                     pd.to_timedelta(schedule_df['DO_time'].dt.hour, unit='h') + \
                                     pd.to_timedelta(schedule_df['DO_time'].dt.minute, unit='m')

    schedule_df['PU_DOW'] = df['LoadDate'].dt.dayofweek
    schedule_df['DO_DOW'] = df['DO_LoadByDate'].dt.dayofweek

    schedule_df['PU_Appt'] = pd.NaT
    pu_ind = schedule_df['PU_ScheduleType'].values == 1
    schedule_df.loc[pu_ind, 'PU_Appt'] = np.where(schedule_df.loc[pu_ind, 'PU_time'] >= schedule_df.loc[pu_ind, 'LoadDate'],
                                                  pd.to_datetime(schedule_df.loc[pu_ind, 'PU_time']), pd.NaT)
    schedule_df.loc[~pu_ind, 'PU_Appt'] = schedule_df.loc[~pu_ind, 'PU_appt_nonschedule']

    schedule_df['DO_Appt'] = pd.NaT
    do_ind = schedule_df['DO_ScheduleType'] == 1
    schedule_df.loc[do_ind, 'DO_Appt'] = np.where(schedule_df.loc[do_ind, 'DO_time'] >= schedule_df.loc[do_ind, 'LoadDate'],
                                                  pd.to_datetime(schedule_df.loc[do_ind, 'DO_time']), pd.NaT)
    schedule_df.loc[do_ind, 'DO_Appt'] = schedule_df.loc[~do_ind, 'DO_appt_nonschedule']

    LOGGER.info('Preprocessing Live Data, Available loads...')
    # drop_features = ['LoadDate','PU_time','PU_ScheduleCloseTime','PU_appt_nonschedule',
    #                'DO_time','DO_ScheduleCloseTime','DO_appt_nonschedule'  ]

    # df.drop(drop_features, axis=1, inplace=True)
    LOGGER.info('Loading Live Data End, Available loads...')
    newloads_df = assign_latlong_bycity(schedule_df, city_df)
    newloads_df = encode_load(newloads_df, 'EquipmentType')

    return newloads_df


def process_histloads(df, city_df, cluster_df):
    LOGGER.info('searching for lat and long')
    name_mapper_origin = {'Latitude': 'OriginLatitude',
                          'Longitude': 'OriginLongitude',
                          'offset': 'PUOffset',
                          'ClusterID': 'OriginClusterID'
                          }
    name_mapper_dest = {
        'Latitude': 'DestinationLatitude',
        'Longitude': 'DestinationLongitude',
        'offset': 'DOOffset',
        'ClusterID': 'DestinationClusterID'
    }
    haul_bucket = [0, 200, 450, 600, 800, 1000, 1200, 1500, 2000, 2500, max(5000, df['Miles'].max())]
    df['haul'] = pd.cut(df['Miles'], bins=haul_bucket).cat.codes
    df['haul'] = df['haul']*10
    df['PU_Arrive'] = pd.to_datetime(df['PU_Arrive'], errors='coerce')
    df['PU_Appt'] = pd.to_datetime(df['PU_Appt'], errors='coerce')
    df['PU_Depart'] = pd.to_datetime(df['PU_Depart'], errors='coerce')
    df['DO_Arrive'] = pd.to_datetime(df['DO_Arrive'], errors='coerce')
    df['PU_Dwell_Minute'] = np.nan
    PU_depart_ind1 = (df['PU_Depart'].values - df['PU_Arrive'].values).astype(np.float32) >= 0
    PU_depart_ind2 = (df['PU_Depart'].values - df['PU_Appt'].values).astype(np.float32) >= 0
    PU_appt_ind = (df['PU_Appt'].values - df['PU_Arrive'].values).astype(np.float32) >= 0

    df['PU_Dwell_Minute'].loc[PU_depart_ind2&PU_appt_ind] = (df['PU_Depart'].loc[PU_depart_ind2 & PU_appt_ind]
                                                         - df['PU_Appt'].loc[PU_depart_ind2 & PU_appt_ind]) / pd.Timedelta('1min')
    df['PU_Dwell_Minute'].loc[PU_depart_ind1 & ~PU_appt_ind] = (df['PU_Depart'].loc[PU_depart_ind1 & ~PU_appt_ind]
                                                           - df['PU_Arrive'].loc[PU_depart_ind1 & ~PU_appt_ind]) / pd.Timedelta('1min')
    df.dropna(inplace=True)
    city_features = ['CityID', 'Latitude', 'Longitude', 'offset']
    df = pd.merge(df, city_df[city_features], left_on=["PUCityID"], right_on=["CityID"], how='left')
    df.drop(columns=['CityID'], inplace=True)
    df = pd.merge(df, cluster_df[['CityID','ClusterID']], left_on=["PUCityID"], right_on=["CityID"], how='left')
    df.drop(columns=['CityID'], inplace=True)
    df = df.rename(columns=name_mapper_origin)
    df = pd.merge(df, city_df[city_features], left_on=["DOCityID"], right_on=["CityID"], how='left')
    df.drop(columns=['CityID'], inplace=True)
    df = pd.merge(df, cluster_df[['CityID','ClusterID']], left_on=["DOCityID"], right_on=["CityID"], how='left')
    df.drop(columns=['CityID'], inplace=True)
    df = df.rename(columns=name_mapper_dest)
    df['PU_Transit_Minute'] = (df['DO_Arrive'] - df['PU_Depart']) / pd.Timedelta('1min') + \
                              (df['DOOffset'] - df['PUOffset']) * 60
    # hour_bucket = [0, 5, 8, 11, 14, 18, 21, 25]
    # df['PU_Bucket'] = pd.cut(df['PU_Hour'], bins=hour_bucket).cat.codes
    # df['DO_Bucket'] = pd.cut(df['DO_Hour'], bins=hour_bucket).cat.codes
    return df


def test_function():
    try:
        city_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl')).reset_index()
        cluster_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_cluster_info.pkl')).reset_index()
    except Exception as e:
        ## Note here we need to identify columns and dtypes
        city_df = pd.DataFrame()
        cluster_df = pd.DataFrame()
        LOGGER.error("Cannot Find city_df File")
        LOGGER.exception(e)

    try:
        test_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'test_data.csv'))
        test_data_processed = process_liveloads(test_data, city_df, cluster_df)
        test_data_processed.to_csv(os.path.join(CONFIG.MODEL_PATH, 'test_data_processed_0408.csv'), index=False)
        LOGGER.info('Test Data Processing Done')
        #print('Test Data Processing Done')
    except Exception as e:
        LOGGER.exception(e)

    try:
        train_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'train_data.csv'))
        train_data = process_histloads(train_data,  city_df, cluster_df)
        train_data.to_csv(os.path.join(CONFIG.MODEL_PATH, 'train_data_processed.csv'), index=False)
        LOGGER.info('Train Data Processing Done')
        print('Train Data Processing Done')
    except Exception as e:
        LOGGER.exception(e)
#test_function()
