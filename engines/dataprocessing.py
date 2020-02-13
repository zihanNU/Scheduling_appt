
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




def process_liveloads(df, city_df):
    LOGGER.info('Loading Live Data, Available loads...')
    #newloads_df = newloads_df[(newloads_df['Division'].isin([1, 2, 3, 4]))& (newloads_df['ShipmentType'].isin([0, 1, 2, 5]))]
    #newloads_df = newloads_df[newloads_df['TotalRate'] > 150]
    df['LoadDate'] = pd.to_datetime(df['LoadDate'], errors='coerce').dt.normalize()
    df['PU_ScheduleCloseTime'] = pd.to_datetime(df['PU_ScheduleCloseTime'], errors='coerce')
    df['DO_ScheduleCloseTime'] = pd.to_datetime(df['DO_ScheduleCloseTime'], errors='coerce')
    df['PU_LoadByDate'] = pd.to_datetime(df['PU_LoadByDate'], errors='coerce').dt.normalize()
    df['DO_LoadByDate'] = pd.to_datetime(df['DO_LoadByDate'], errors='coerce').dt.normalize()
    df['PU_time'] = pd.to_datetime(df['PU_time'], errors='coerce')
    df['DO_time'] = pd.to_datetime(df['DO_time'], errors='coerce')
    df['UpdateDate'] = pd.to_datetime(df['UpdateDate'], errors='coerce')


    # only select those which need schedules
    mask_PU_ind = (df['PU_ScheduleType'].values == 1) & (df['PU_time'] <= df['LoadDate'])
    mask_DO_ind = (df['DO_ScheduleType'].values == 1) & (df['DO_time'] <= df['LoadDate'])

    df = df[mask_PU_ind | mask_DO_ind]

    df['PU_appt_nonschedule'] = df['PU_LoadByDate'].dt.normalize() + \
                                     pd.to_timedelta(df['PU_time'].dt.hour, unit='h') + \
                                     pd.to_timedelta(df['PU_time'].dt.minute, unit='m')

    df['DO_appt_nonschedule'] = df['DO_LoadByDate'].dt.normalize() + \
                                     pd.to_timedelta(df['DO_time'].dt.hour, unit='h') + \
                                     pd.to_timedelta(df['DO_time'].dt.minute, unit='m')

    df['PU_DOW'] = df['LoadDate'].dt.dayofweek
    df['DO_DOW'] = df['DO_LoadByDate'].dt.dayofweek

    df['PU_Appt'] = np.nan
    df.loc[df['PU_ScheduleType'].values == 1, 'PU_Appt'] = df['PU_ScheduleCloseTime']
    df.loc[df['PU_ScheduleType'].values > 1, 'PU_Appt'] = df['PU_appt_nonschedule']

    df['DO_Appt'] = np.nan
    df.loc[df['DO_ScheduleType'] == 1, 'DO_Appt'] = df['DO_ScheduleCloseTime']
    df.loc[df['DO_ScheduleType'] > 1, 'DO_Appt'] = df['DO_appt_nonschedule']

    LOGGER.info('Preprocessing Live Data, Available loads...')
    # drop_features = ['LoadDate','PU_time','PU_ScheduleCloseTime','PU_appt_nonschedule',
    #                'DO_time','DO_ScheduleCloseTime','DO_appt_nonschedule'  ]

    # df.drop(drop_features, axis=1, inplace=True)
    LOGGER.info('Loading Live Data End, Available loads...')
    newloads_df = assign_latlong_bycity(df, city_df)
    newloads_df = encode_load(newloads_df, 'EquipmentType')

    return newloads_df


def process_histloads(df, city_df):
    LOGGER.info('searching for lat and long')
    name_mapper_origin = {'Latitude': 'OriginLatitude',
                          'Longitude': 'OriginLongitude',
                          'offset': 'PUOffset'
                          }
    name_mapper_dest = {
        'Latitude': 'DestinationLatitude',
        'Longitude': 'DestinationLongitude',
        'offset': 'DOOffset'
    }
    df['PU_Arrive'] = pd.to_datetime(df['PU_Arrive'], errors='coerce')
    df['PU_Depart'] = pd.to_datetime(df['PU_Depart'], errors='coerce')
    df['PU_Dwell_Minute'] = (df['PU_Depart'] - df['PU_Arrive']) / pd.Timedelta('1min')
    df['DO_Arrive'] = pd.to_datetime(df['DO_Arrive'], errors='coerce')
    city_features = ['CityID', 'Latitude', 'Longitude', 'offset']
    df = pd.merge(df, city_df[city_features], left_on=["PUCityID"], right_on=["CityID"], how='left')
    df.drop(columns=['CityID'], inplace=True)
    df = df.rename(columns=name_mapper_origin)
    df = pd.merge(df, city_df[city_features], left_on=["DOCityID"], right_on=["CityID"], how='left')
    df = df.rename(columns=name_mapper_dest)
    df.drop(columns=['CityID'], inplace=True)
    df['PU_Transit_Minute'] = (df['DO_Arrive'] - df['PU_Depart']) / pd.Timedelta('1min') + \
                              (df['DOOffset'] - df['PUOffset']) * 60
    # hour_bucket = [0, 5, 8, 11, 14, 18, 21, 25]
    # df['PU_Bucket'] = pd.cut(df['PU_Hour'], bins=hour_bucket).cat.codes
    # df['DO_Bucket'] = pd.cut(df['DO_Hour'], bins=hour_bucket).cat.codes


    return df


def test_function():
    try:
        city_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl')).reset_index()
    except Exception as e:
        city_df = pd.DataFrame()
        LOGGER.error("Cannot Find city_df File")
        LOGGER.exception(e)

    try:
        test_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'test_data.csv'))
        test_data_processed = process_liveloads(test_data, city_df)
        test_data_processed.to_csv(os.path.join(CONFIG.MODEL_PATH, 'test_data_processed.csv'), index=False)
        LOGGER.info('Test Data Processing Done')
        print('Test Data Processing Done')
    except Exception as e:
        LOGGER.exception(e)

    try:
        train_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'train_data.csv'))
        train_data = process_histloads(train_data,  city_df)
        train_data.to_csv(os.path.join(CONFIG.MODEL_PATH, 'train_data_processed.csv'), index=False)
        LOGGER.info('Train Data Processing Done')
        print('Train Data Processing Done')
    except Exception as e:
        LOGGER.exception(e)

