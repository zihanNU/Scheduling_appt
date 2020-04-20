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
from engines.dynamic_cache import get_liveloads
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
        histloads_df = init_read_histload('train_data_processed_cv.csv')
        facility_hour_df = init_read_facility('facility_hour.pkl')
        newloads_df = init_read_liveload('test_data_processed_cv.csv')
        #newloads_df = get_liveloads()
    except Exception as e:
        LOGGER.exception(e)

    LOGGER.info("*** System Initialization Done ***")
    #newloads_part1_id = newloads_df.loc[newloads_part1_ind, 'LoadID'].tolist()

    results_df1, results_df2 = scheduler_model(newloads_df, histloads_df)
    # results_df1.to_csv('result1_cv.csv', index=False)
    # results_df2.to_csv('result2_cv.csv', index=False)
    features = ['LoadID', 'Miles', 'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt', 'PU_Date',
                'pu_scheduletime', 'pu_schedulehour',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'DO_Date', 'do_scheduletime', 'do_schedulehour']
    results_df1 = scheduler_spread(results_df1)  #after this step, reset the column names of df1 into df2.
    results_df = pd.concat([results_df1, results_df2], axis=0, ignore_index=True)
    features = ['LoadID', 'Miles', 'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt',
                'pu_scheduletime', 'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime',
                'PUopen', 'PUclose', 'DOopen', 'DOclose']
    scheduler_results_df = feasibility_check(results_df, facility_hour_df)
    scheduler_results_df[features].to_csv(os.path.join(CONFIG.MODEL_PATH, 'test_results_cv.csv'), index=False)



