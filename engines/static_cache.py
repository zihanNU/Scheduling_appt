import os
import time
import logging

import pandas as pd
import numpy as np


from engines.query import QueryEngine
from engines.offset_functions import offset
from engines.dataprocessing import process_histloads

import config

LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()
QUERY = QueryEngine()


BACKOFF_FACTOR = 5
MAX_BACKOFF    = 900  # 15 minutes in seconds


def daily_update():
    LOGGER.info('Daily Update Historical Data with Stored Procedure...')
    time_start = time.time()
    try:
        QUERY.daily_update_HistLoad()
    except Exception as ex:
        LOGGER.exception("Exception while running daily_update_HistLoad() (took {:.2f}s): {}".format(
            time.time() - time_start, repr(ex)))
        LOGGER.warning("Skipping daily_update_CorridorMargin() due to failed daily_update_HistLoad()")

    LOGGER.info("Completed daily_update_HistLoad() in {:.2f}s".format(time.time() - time_start))

    LOGGER.info("Completed daily_update in {:.2f}s".format(time.time() - time_start))
    return


def get_facility_hour(facility_hour):
    facility_hour['Tag'] = 0
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    for day in days:
        opentime = day + 'Open'
        closetime = day + 'Close'
        openhour = day + 'Openhour'
        closehour = day + 'Closehour'
        facility_hour[openhour] = pd.to_datetime(facility_hour[opentime]).dt.hour + pd.to_datetime(
            facility_hour[opentime]).dt.minute / 60.0
        facility_hour[closehour] = pd.to_datetime(facility_hour[closetime]).dt.hour + pd.to_datetime(
            facility_hour[closetime]).dt.minute / 60.0
        closedate_ind = facility_hour[openhour] == facility_hour[closehour]
        facility_hour.loc[~closedate_ind, closehour] = \
            facility_hour.loc[~closedate_ind, closehour].replace(0.0, 23.99999)
        facility_hour.loc[closedate_ind, 'Tag'] = facility_hour.loc[closedate_ind, 'Tag'] + 1
        facility_hour.drop(columns=[opentime, closetime], inplace=True)
    return facility_hour


def build_pickle_facility_df():
    facility_hour = None
    backoff_index = 0
    while facility_hour is None:
        try:
            facility_info = QUERY.get_facility_initial()
            facility_info = facility_info.sort_values(['FacilityID', 'UpdateDate'], ascending=True).\
                drop_duplicates(subset=['FacilityID'], keep='last')
            facility_file = os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_facility_info.pkl')
            if os.path.exists(facility_file) and os.path.getsize(facility_file) > 100:
                facility_origin = pd.read_pickle(facility_file)
                facility_origin = facility_origin.sort_values(['FacilityID', 'UpdateDate'], ascending=True).drop_duplicates(
                    subset=['FacilityID'], keep='last')
                max_timestamp = facility_origin.UpdateDate.max()
                facilityinfo_new = facility_info.loc[facility_info['UpdateDate'] > max_timestamp]
                if 'Tag' not in facility_origin.columns:
                    facility_data = get_facility_hour(facility_info)
                elif facilityinfo_new.shape[0] > 0:
                    facility_datanew = get_facility_hour(facilityinfo_new)
                    facility_data = pd.concat([facility_origin, facility_datanew], axis=0, ignore_index=True)
                else:
                    facility_origin = facility_origin.sort_values('UpdateDate', ascending=True).drop_duplicates(
                        subset=['FacilityID'], keep='last')
                    facility_origin.to_pickle(
                        os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_facility_info.pkl'))
                facility_data = facility_data.sort_values(['FacilityID', 'UpdateDate'], ascending=True).drop_duplicates(
                    subset=['FacilityID'], keep='last')
            else:
                facility_data = get_facility_hour(facility_info)
            facility_data.set_index('FacilityID', drop=True, inplace=True)
            facility_data.to_pickle('app_scheduler_facility_info.pkl')
        except Exception as ex:
            LOGGER.exception("Exception while running get_cluster_initial(): (attempt=={}): {}".format(
                backoff_index, repr(ex)))
            backoff_index += 1  # begin at BACKOFF_FACTOR seconds
            time.sleep(min(BACKOFF_FACTOR**backoff_index, MAX_BACKOFF))


def build_pickle_offset_initial():
    try:
        cityinfo = QUERY.get_cityinfo_initial()
        cityinfo = cityinfo.sort_values(['CityID', 'UpdateDate'], ascending=True).drop_duplicates(subset=['CityID'], keep='last')
        city_file = os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl')
        if os.path.exists(city_file) and os.path.getsize(city_file) > 100:
            cityinfo_origin = pd.read_pickle(city_file)
            cityinfo_origin = cityinfo_origin.sort_values(['CityID', 'UpdateDate'], ascending=True).drop_duplicates(subset=['CityID'], keep='last')
            max_timestamp = cityinfo_origin.UpdateDate.max()
            cityinfo_new = cityinfo.loc[cityinfo['UpdateDate'] > max_timestamp]
            if 'offset' not in cityinfo_origin.columns:
                cityinfo['offset'] = cityinfo.apply(
                    lambda x: offset(x['TimeZone']), axis=1)
            elif cityinfo_new.shape[0] > 0:
                cityinfo_new['offset'] = cityinfo_new.apply(
                    lambda x: offset(x['TimeZone']), axis=1)
                cityinfo = pd.concat([cityinfo_origin, cityinfo_new], axis=0).reset_index(drop=True)
            else:
                cityinfo['StateID'].fillna(-99, inplace=True)
                dtype_dict = {'CityID': np.int32, 'StateID': np.int32, 'offset': np.float32}
                cityinfo_origin = cityinfo_origin.astype(dtype_dict)
                cityinfo_origin = cityinfo_origin.sort_values('UpdateDate', ascending=True).drop_duplicates(subset=['CityID'], keep='last')
                cityinfo_origin.to_pickle(
                    os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
                return cityinfo_origin
            cityinfo = cityinfo.sort_values(['CityID', 'UpdateDate'], ascending=True).drop_duplicates(subset=['CityID'], keep='last')
        else:
            cityinfo['offset'] = cityinfo.apply(
                lambda x: offset(x['TimeZone'], CONFIG.TIMEZONEBASE), axis=1)
        cityinfo['StateID'].fillna(-99, inplace=True)
        dtype_dict = {'CityID': np.int32, 'StateID': np.int32, 'offset': np.float32}
        cityinfo = cityinfo.astype(dtype_dict)
        cityinfo.to_pickle(
            os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
        return cityinfo
    except Exception as ex:
        LOGGER.exception("Exception while running get_cityinfo_initial(): {}".format(repr(ex)))
        LOGGER.warn("Continuing with empty cargolimit df")


def build_pickle_cluster_info():
    cluster_info = None
    backoff_index = 0
    while cluster_info is None:
        try:
            cluster_info = QUERY.get_cluster_initial()
        except Exception as ex:
            LOGGER.exception("Exception while running get_cluster_initial(): (attempt=={}): {}".format(
                backoff_index, repr(ex)))
            backoff_index += 1  # begin at BACKOFF_FACTOR seconds
            time.sleep(min(BACKOFF_FACTOR**backoff_index, MAX_BACKOFF))
    cluster_info['ClusterID'] = cluster_info['ClusterID'].fillna(-99).astype(np.int32)
    cluster_info.to_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_cluster_info.pkl'))
    return cluster_info


def build_pickle_hist_loads_df(city_df, cluster_df):
    hist_loads = None
    backoff_index = 0
    while hist_loads is None:
        try:
            hist_loads = QUERY.get_histload_initial()
        except Exception as ex:
            LOGGER.exception("Exception while running get_carrier_histload_initial(): (attempt=={}): {}".format(
                backoff_index, repr(ex)))
            backoff_index += 1  # begin at BACKOFF_FACTOR seconds
            time.sleep(min(BACKOFF_FACTOR**backoff_index, MAX_BACKOFF))

    LOGGER.info('Successfully read carrier_loads_df, processing..')
    ### This part is dependent on new sp
    hist_loads_df = process_histloads(hist_loads, city_df, cluster_df)
    hist_loads_df.to_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_histloads.pkl'))
    return


def hist_cache():
    os.makedirs(CONFIG.MODEL_PATH, exist_ok=True)
    daily_update()
    LOGGER.info('Update Historical Data...')
    #### Initilizer Start#####

    LOGGER.info('Cache city data...')
    city_df = build_pickle_offset_info()
    cluster_df = build_pickle_cluster_info()
    LOGGER.info('Cache load history data...')
    build_pickle_hist_loads_df(city_df, cluster_df)
    LOGGER.info('Cache facility data...')
    build_pickle_facility_df()
    LOGGER.info("Successfully cached history")

def main():
    if not os.path.exists(CONFIG.MODEL_PATH):
        os.makedirs(CONFIG.MODEL_PATH)
    hist_cache()


if __name__ == '__main__':
    main()