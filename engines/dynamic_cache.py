import os
import logging

import pandas as pd

import config
from engines.dataprocessing import process_liveloads
from engines.query import QueryEngine


LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()
QUERY = QueryEngine()


def main():
    #for independent test, to make this file run independent from trucknorris_live_sql.py to save testing time
    if not os.path.exists(CONFIG.MODEL_PATH):
        os.makedirs(CONFIG.MODEL_PATH)
    city_info = pd.read_pickle(
            os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
    newloads_df = QUERY.get_liveload()
    loads_df = process_liveloads(newloads_df, city_info)
    loads_df.to_pickle(os.path.join(CONFIG.MODEL_PATH, 'live_bazooka_loads.pkl'))
    LOGGER.info('Loading Live Data Done...')
    return loads_df


if __name__ == '__main__':
    main()


def get_liveloads():
    if not os.path.exists(CONFIG.MODEL_PATH):
        os.makedirs(CONFIG.MODEL_PATH)
    city_info = pd.read_pickle(
            os.path.join(CONFIG.MODEL_PATH, 'app_scheduler_city_info.pkl'))
    try:
        newloads_df = QUERY.get_liveload()
    except Exception as e:
        LOGGER.exception(e)
    load_file = os.path.join(CONFIG.MODEL_PATH, 'df_live_bazooka_loads.pkl')
    if os.path.exists(load_file):
        loads_df = pd.read_pickle(load_file)
        updatedatetime = loads_df.UpdateDate.max()
        newloads_df = newloads_df.loc[newloads_df[updatedatetime] > updatedatetime]
        loads_df_new = process_liveloads(newloads_df, city_info)
        loads_df = pd.concat([loads_df, loads_df_new], axis=0)
        loads_df.reset_index(drop=True, inplace=True)
    else:
        loads_df = process_liveloads(newloads_df, city_info)

    loads_df.to_pickle(os.path.join(CONFIG.MODEL_PATH, 'df_live_bazooka_loads_cf.pkl'))
    LOGGER.info('Loading Live Data Done...')
    return loads_df


