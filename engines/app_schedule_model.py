import os
import pandas as pd
import datetime
from engines.scheduling_model import scheduler_model
from engines.spread_function import scheduler_spread
from engines.feasibility_function import feasibility_check

import config
import logging


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
        results_df['LoadDate'] = results_df['LoadDate'].apply(lambda x: x.strftime("%Y-%m-%d"))
        results_df['PU_ScheduleTime'] = results_df['PU_ScheduleTime'].apply(lambda x: x.strftime("%Y-%m-%d %R"))
        results_df['DO_ScheduleTime'] = results_df['DO_ScheduleTime'].apply(lambda x: x.strftime("%Y-%m-%d %R"))
        return results_df.to_dict('records'), status


def schedule_mimic(newloads_df, histloads_df, facility_hour_df, loadID, pre_results):
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
    final_results = pd.concat([scheduler_results_df[api_features], pre_results], axis=0)
    final_results.drop_duplicates(subset='LoadID', inplace=True)
    final_results.reset_index(drop=True, inplace=True)
    filename_APPT = 'app_scheduler_results{0}.pkl'.format(datetime.datetime.now().strftime('%Y-%m-%d'))
    final_results.to_pickle(os.path.join(CONFIG.MODEL_PATH, filename_APPT))

    LOGGER.info("END: Mimic Scheduling Process")

    result_json, status = api_json_output(final_results)
    LOGGER.info("Finish to Process for api at time {0}".format(datetime.datetime.now()))
    return result_json, status
