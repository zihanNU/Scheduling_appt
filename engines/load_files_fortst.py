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
    facility_info = QUERY.get_facility_initial()
    facility_info.iloc[0:1000].to_pickle(
        os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_facility_info_test.pkl'))



def build_pickle_hist_loads_df():
    hist_loads = QUERY.get_histload_initial()
    hist_loads.iloc[0:1000].to_pickle(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_histloads_test.pkl'))
    return


def hist_cache():
    os.makedirs(CONFIG.MODEL_PATH, exist_ok=True)
    build_pickle_hist_loads_df()
    LOGGER.info('Cache facility data...')
    build_pickle_facility_df()
    LOGGER.info("Successfully cached history")

def main():
    if not os.path.exists(CONFIG.MODEL_PATH):
        os.makedirs(CONFIG.MODEL_PATH)
    hist_cache()


if __name__ == '__main__':
    main()