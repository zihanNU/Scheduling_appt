"""
Created on May 9 2018
Merged on June 20 2019
Author: Zihan Hong

Notes: if we want objected orient programming.
        Initilizer needs three main input: carrier_load ,trucks_df,newloads_df;
                trucks_df can be from truck_listing or specific match_api_get
                _logger_ can be also tagged as input
        output: results_df
"""

import os

import pandas as pd
import datetime
from engines.initialization import init_read_histload, init_read_facility, init_read_liveload
from engines.query import QueryEngine
from engines.scheduling_model import scheduler_model
from engines.dataprocessing import process_liveloads
from engines.spread_function import scheduler_spread
from engines.feasibility_function import feasibility_check

import config
import logging

now = datetime.datetime.now()
QUERY = QueryEngine()



CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    LOGGER.info("*** System Initialization ***")

    try:
        histloads_df = init_read_histload()
        facility_hour_df = init_read_facility()
        newloads_df = init_read_liveload()
        #newloads_df = get_liveloads()
    except Exception as e:
        LOGGER.exception(e)

    LOGGER.info("*** System Initialization Done ***")
    #newloads_part1_id = newloads_df.loc[newloads_part1_ind, 'LoadID'].tolist()

    results_df1, results_df2 = scheduler_model(newloads_df, histloads_df)
    results_df1.to_csv('result1.csv', index=False)
    results_df2.to_csv('result2.csv', index=False)

    results_df1 = scheduler_spread(results_df1)  #after this step, reset the column names of df1 into df2.
    results_df = pd.concat([results_df1, results_df2], axis=0, ignore_index=True)
    features = ['LoadID', 'Miles', 'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt',
                'pu_scheduletime',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime']
    scheduler_results_df = feasibility_check(results_df, facility_hour_df)[features]
    scheduler_results_df.to_csv(os.path.join(CONFIG.MODEL_PATH, 'test_results.csv'), index=False)


