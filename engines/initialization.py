import config
import pandas as pd
import logging
import os


LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()


def init_read_histload(filename):
    features = ['LoadID', 'LoadDate', 'LoadEquipment', 'Miles', 'TotalWeight',
                'CustomerID', 'PU_Facility', 'DO_Facility', 'PUCityID', 'PU_ScheduleType',
                'DOCityID', 'DO_ScheduleType', 'PU_Hour', 'DO_Hour', 'PU_Bucket', 'DO_Bucket', 'haul',
                'PU_Dwell_Minute', 'OriginLatitude', 'OriginLongitude', 'PUOffset',
                'OriginClusterID', 'DestinationLatitude', 'DestinationLongitude',
                'DOOffset', 'DestinationClusterID', 'PU_Transit_Minute']
    try:
        hist_data_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, filename))
        hist_data_df.rename(columns={'PU_FacilityID': 'PU_Facility', 'DOAppt': 'DO_Appt', 'DO_FacilityID': 'DO_Facility',
                                  'PUScheduleType': 'PU_ScheduleType',  'DOScheduleType': 'DO_ScheduleType'},
                         inplace=True)
    except Exception as e:
        hist_data_df = pd.DataFrame(columns=[features])
        LOGGER.error("Cannot Find hist load File")
        LOGGER.exception(e)
    return hist_data_df[features]


def init_read_liveload(filename):
    features = ['LoadID', 'LoadDate', #'EquipmentType',
                'Miles', 'TotalWeight',
                'CustomerID', 'PU_Facility', 'DO_Facility', 'PUCityID', 'PU_ScheduleType',
                'DOCityID', 'DO_ScheduleType', 'PU_Appt', 'DO_Appt', 'haul',
                'OriginLatitude', 'OriginLongitude', 'PUOffset', 'DestinationLatitude',
                'DestinationLongitude', 'DOOffset', 'OriginClusterID', 'DestinationClusterID']
    try:
        live_data_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, filename))

    except Exception as e:
        live_data_df = pd.DataFrame(columns=features)
        LOGGER.error("Cannot Find live load File")
        LOGGER.exception(e)
    return live_data_df#.loc[live_data['LoadID'].isin([17481586, 17481590]), features]


def init_read_facility(filename):
    try:
        facility_hour = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, filename))
        facility_hour_df = facility_hour[facility_hour['Tag'] < 7]
    except Exception as e:
        facility_hour_df = pd.DataFrame()
        LOGGER.error("Cannot Find facility_hour File")
        LOGGER.exception(e)
    return facility_hour_df


def init_read_preresults(filename):
    features = ['LoadID', 'LoadDate', 'PU_Facility', 'PU_ScheduleTime', 'DO_Facility', 'DO_ScheduleTime']
    try:
        preresults_df = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, filename))
    except Exception as e:
        preresults_df = pd.DataFrame(columns=features)
        LOGGER.error("Cannot Find previous results File")
        LOGGER.exception(e)
    return preresults_df
