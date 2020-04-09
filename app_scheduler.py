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

import config
import logging

now = datetime.datetime.now()
QUERY = QueryEngine()



CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)



def scheduler_feasibility(newloads_df, facility_hour_df):
    return None

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
    newload_results_df = scheduler_model(newloads_df, histloads_df)

