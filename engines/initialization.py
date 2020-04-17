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
        hist_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, filename))
        hist_data.rename(columns={'PU_FacilityID': 'PU_Facility', 'DOAppt': 'DO_Appt', 'DO_FacilityID': 'DO_Facility',
                                  'PUScheduleType': 'PU_ScheduleType',  'DOScheduleType': 'DO_ScheduleType'},
                         inplace=True)
    except Exception as e:
        hist_data = pd.DataFrame(columns=[features])
        LOGGER.error("Cannot Find train_data File")
        LOGGER.exception(e)
    return hist_data[features]


def init_read_liveload(filename):
    features = ['LoadID', 'LoadDate', #'EquipmentType',
                'Miles', 'TotalWeight',
                'CustomerID', 'PU_Facility', 'DO_Facility', 'PUCityID', 'PU_ScheduleType',
                'DOCityID', 'DO_ScheduleType', 'PU_Appt', 'DO_Appt', 'haul',
                'OriginLatitude', 'OriginLongitude', 'PUOffset', 'DestinationLatitude',
                'DestinationLongitude', 'DOOffset', 'OriginClusterID', 'DestinationClusterID']
    try:
        live_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, filename))

    except Exception as e:
        live_data = pd.DataFrame(columns=features)
        LOGGER.error("Cannot Find test_data File")
        LOGGER.exception(e)
    return live_data.loc[live_data['LoadID'].isin([17900642]), features]


def init_read_facility(filename):
    try:
        facility_hour = pd.read_pickle(os.path.join(CONFIG.MODEL_PATH, filename))
    except Exception as e:
        facility_hour = pd.DataFrame()
        LOGGER.error("Cannot Find facility_hour File")
        LOGGER.exception(e)
    return facility_hour[facility_hour['Tag'] < 7]

