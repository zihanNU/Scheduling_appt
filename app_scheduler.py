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
from engines.initialization import init_read_histload
from engines.initialization import init_read_facility, init_read_liveload
from engines.query import QueryEngine
from engines.scheduling_model import scheduler_ml, scheduler_rule
#from engines.dataprocessing import process_liveloads

import config
import logging

now = datetime.datetime.now()
QUERY = QueryEngine()



CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)

Weekday_Mapper = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}


def scheduler_feasibility(newloads_df,facility_hour_df):
    return None





if __name__ == '__main__':
    LOGGER.info("*** System Initialization ***")

    try:
        histloads_df = init_read_histload()
        facility_hour_df = init_read_facility()
        newloads_df = init_read_liveload()
    except Exception as e:
        LOGGER.exception(e)

    LOGGER.info("*** System Initialization Done ***")

    newloads_part1_df = newloads_df.loc[(newloads_df['PU_Appt'].isna()) & (newloads_df['DO_Appt'].isna())]
    newloads_part2_df = newloads_df.loc[~newloads_df['LoadID'].values.isin(newloads_part1_df['LoadID'].values)]

    newloads_part1_scheduling_df = scheduler_ml(newloads_part1_df, histloads_df )
    newloads_part2_scheduling_df = scheduler_rule(newloads_part2_df, histloads_df)
    newloads_result_df = pd.concat([newloads_part1_scheduling_df, newloads_part2_scheduling_df])
    #newloads_result_df = scheduler_feasibility(newloads_result_df, facility_hour_df)
