import os
import time
import logging

import pandas as pd


from engines.query import QueryEngine
from engines.offset_functions import offset
from engines.dataprocessing import  process_histloads

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

def build_pickle_offset_info():
    try:
        cityinfo = QUERY.get_cityinfo_initial()
        cityinfo = cityinfo.sort_values('UpdateDate', ascending=True).drop_duplicates(subset=['CityID'], keep='last')
        if os.path.exists(os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl')):
            cityinfo_origin = pd.read_pickle(
                os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
            cityinfo_origin = cityinfo_origin.sort_values('UpdateDate', ascending=True).drop_duplicates(subset=['CityID'], keep='last')
            max_timestamp = cityinfo_origin.UpdateDate.max()
            cityinfo_new = cityinfo.loc[cityinfo['UpdateDate']>max_timestamp]
            if 'offset' not in cityinfo_origin.columns:
                cityinfo['offset'] = cityinfo.apply(
                    lambda x: offset(x['TimeZone']), axis=1)
            elif cityinfo_new.shape[0] > 0:
                cityinfo_new['offset'] = cityinfo_new.apply(
                    lambda x: offset(x['TimeZone']), axis=1)
                cityinfo = pd.concat([cityinfo_origin,cityinfo_new]).reset_index(drop=True)
            else:
                cityinfo_origin.to_pickle(
                    os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
                return cityinfo_origin
        else:
            cityinfo['offset'] = cityinfo.apply(
                lambda x: offset(x['TimeZone']), axis=1)
        cityinfo.to_pickle(
            os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
    except Exception as ex:
        LOGGER.exception("Exception while running get_cityinfo_initial(): {}".format(repr(ex)))
    return cityinfo

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
    #daily_update()
    LOGGER.info('Update Historical Data...')
    #### Initilizer Start#####

    LOGGER.info('Cache city data...')
    city_df = build_pickle_offset_info()
    cluster_df = build_pickle_cluster_info()
    LOGGER.info('Cache load history data...')
    build_pickle_hist_loads_df(city_df)

    LOGGER.info("Successfully cached history")

def main():
    if not os.path.exists(CONFIG.MODEL_PATH):
        os.makedirs(CONFIG.MODEL_PATH)
    hist_cache()


if __name__ == '__main__':
    main()