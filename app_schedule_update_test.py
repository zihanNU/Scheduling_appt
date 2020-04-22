"""
Created on April 9 2020
Author: Zihan Hong
"""

from flask import Flask, jsonify, request
import numpy as np

import traceback
import os

import pandas as pd
import datetime
from engines.initialization import init_read_histload, init_read_facility, init_read_liveload
#from engines.query import QueryEngine
from engines.scheduling_model import scheduler_model
from engines.dynamic_cache import get_liveloads
from engines.spread_function import scheduler_spread
from engines.feasibility_function import feasibility_check
from engines.case_insensitive_dict import CaseInsensitiveDict
from engines.app_schedule_model import schedule_mimic

import config
import logging

now = datetime.datetime.now()
#QUERY = QueryEngine()

app = Flask(__name__)

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)


def api_json_output(results_df):
    """
    Prepares the results for json output back to the requestor.

    Args:
        results_df: data frame with the results
    """
   # This part is to match the 1000 loads as _loadSizeReturn
    if (len(results_df)==0):
        #return newload_df.iloc[:CONFIG._loadSizeReturn][["LoadID", "Reason", "Score"]].to_dict('records')
        ### Until here. if we do not need to have 1000 loads requirement for response. we can return an empty list
        status = 'no specific load detected'
        return [], status
    else:
        results_df.sort_values(by=['LoadDate', 'PU_Facility', 'DO_Facility'], inplace=True)
        results_df.reset_index(drop=True, inplace=True)
        status = 'load scheduling done'
        results_df['PU_ScheduleTime'] = results_df['PU_ScheduleTime'].apply(lambda x: x.strftime("%Y-%m-%d %R"))
        results_df['DO_ScheduleTime'] = results_df['DO_ScheduleTime'].apply(lambda x: x.strftime("%Y-%m-%d %R"))
        return results_df.to_dict('records'), status


def create_app():
    app = Flask(__name__)

    LOGGER.info("*** System Initialization ***")
    global HISTLOAD_DF
    global FACILITY_HOUR_DF
    global NEWLOAD_DF
    try:
        # histloads_df = init_read_histload('train_data.pkl')
        # facility_hour_df = init_read_facility('facility_hour.pkl')
        # newloads_df = init_read_liveload('test_data.pkl')
        HISTLOAD_DF = init_read_histload('train_data_processed_cv.csv')
        FACILITY_HOUR_DF = init_read_facility('app_scheduler_facility_info.pkl')
        NEWLOAD_DF = init_read_liveload('test_data_processed_cv.csv')
            #init_read_liveload('test_data_processed_cv.csv')
        LOGGER.info("*** System Ready for Requests ***")

    except Exception as e:
        LOGGER.exception(e)
    return app

app = create_app()

@app.route('/schedule_mimic/', methods=['GET'], strict_slashes=False)
def scheduler():
    global HISTLOAD_DF
    global FACILITY_HOUR_DF
    global NEWLOAD_DF

    try:
        LOGGER.info("Start to Process for api at time {0}".format(datetime.datetime.now()))
        values = CaseInsensitiveDict(request.values.to_dict())
        loadID = values.get('LoadID', type=int, default=0)

        result_json, status = schedule_mimic(newloads_df=NEWLOAD_DF, histloads_df=HISTLOAD_DF,
                                             facility_hour_df=FACILITY_HOUR_DF, loadID=loadID)

        LOGGER.info("END: Mimic Scheduling Process")

        LOGGER.info("Finish to Process for api at time {0}".format(datetime.datetime.now()))

        return jsonify({'Loads': result_json, "Version": CONFIG.API_VERSION, "Status": status})

    except Exception as ex:
        LOGGER.error(ex)


if __name__ == '__main__':
    app.run(debug=True)
