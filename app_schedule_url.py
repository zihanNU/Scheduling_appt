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
from engines.query import QueryEngine
from engines.scheduling_model import scheduler_model
from engines.dynamic_cache import get_liveloads
from engines.spread_function import scheduler_spread
from engines.feasibility_function import feasibility_check
from engines.case_insensitive_dict import CaseInsensitiveDict


import config
import logging

now = datetime.datetime.now()
QUERY = QueryEngine()

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

@app.route('/scheduleing_mimic/', methods=['GET'], strict_slashes=False)
def scheduler():
    try:
        LOGGER.info("Start to Process for api at time {0}".format(datetime.datetime.now()))
        values = CaseInsensitiveDict(request.values.to_dict())
        loadID = values.get('LoadID', type=int, default=0)
        if loadID > 0:
            newloads_url_df = newloads_df.loc[newloads_df['LoadID'] == loadID]
            results_df1, results_df2 = scheduler_model(newloads_url_df, histloads_df)
        else:
            results_df1, results_df2 = scheduler_model(newloads_df, histloads_df)
        results_df1 = scheduler_spread(results_df1)  # after this step, reset the column names of df1 into df2.
        results_df = pd.concat([results_df1, results_df2], axis=0, ignore_index=True)
        scheduler_results_df = feasibility_check(results_df, facility_hour_df)

        col_rename = {'pu_scheduletime': 'PU_ScheduleTime', 'do_scheduletime': 'DO_ScheduleTime'}
        scheduler_results_df.rename(columns=col_rename, inplace=True)
        api_features = ['LoadID', 'LoadDate', 'PU_Facility', 'PU_ScheduleTime', 'DO_Facility', 'DO_ScheduleTime']
        #scheduler_results_df[features].to_csv(os.path.join(CONFIG.MODEL_PATH, 'test_results_cv.csv'), index=False)

        LOGGER.info("END: Mimic Scheduling Process")

        result_json, status = api_json_output(scheduler_results_df[api_features])

        return jsonify({'Loads': result_json, "Version": CONFIG.API_VERSION, "Status": status})
        LOGGER.info("Finish to Process for api at time {0}".format(datetime.datetime.now()))


    except Exception as ex:
        LOGGER.error(ex)


LOGGER.info("*** System Initialization ***")

try:
    # histloads_df = init_read_histload('train_data.pkl')
    # facility_hour_df = init_read_facility('facility_hour.pkl')
    # newloads_df = init_read_liveload('test_data.pkl')
    histloads_df = init_read_histload('train_data_processed_cv.csv')
    facility_hour_df = init_read_facility('facility_hour.pkl')
    newloads_df = init_read_liveload('test_data_processed_cv.csv')
    LOGGER.info("*** System Ready for Requests ***")

except Exception as e:
    LOGGER.exception(e)


if __name__ == '__main__':
    app.run(debug=True)
