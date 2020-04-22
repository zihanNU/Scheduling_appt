"""
Created on April 9 2020
Author: Zihan Hong
"""

from flask import Flask, jsonify, request
import atexit
import threading
import datetime
from engines.initialization import init_read_histload, init_read_facility, init_read_liveload, init_read_preresults
from engines.query import QueryEngine
from engines.app_schedule_model import schedule_mimic
from engines.dynamic_cache import get_liveloads
from engines.case_insensitive_dict import CaseInsensitiveDict
from engines.static_cache import hist_cache


import config
import logging

QUERY = QueryEngine()

CONFIG = config.Config()

LOGGER = logging.getLogger(__name__)

LOGGER.info("** API Starting **")

# lock to control access to variable
dataLock = threading.Lock()
# thread handler
DATA_UPDATE_THREAD = threading.Thread()
POOL_TIME_LIVE_LOADS = 1800 # 30 minutes

def create_app():
    get_liveloads()  #

    app = Flask(__name__)

    def interrupt():
        """
        Kill thread processes if interruptted.
        """
        global DATA_UPDATE_THREAD

        LOGGER.info('Interrupted, killing thread processes')
        DATA_UPDATE_THREAD.cancel()

    def first_time_read():
        global HISTLOAD_DF
        global FACILITY_HOUR_DF
        global NEWLOAD_DF
        global DATA_UPDATE_THREAD
        global SCHEDULE_APPT
        with dataLock:
            try:
                HISTLOAD_DF = init_read_histload('app_scheduler_histloads.pkl')
                FACILITY_HOUR_DF = init_read_facility('app_scheduler_facility_info.pkl')
                NEWLOAD_DF = init_read_liveload('app_scheduler_live_loads.pkl')
                filename_APPT = 'app_scheduler_results{0}.pkl'.format(datetime.datetime.now().strftime('%Y-%m-%d'))
                SCHEDULE_APPT = init_read_preresults(filename_APPT)
                #get_liveloads()

                LOGGER.info("*** System Ready for Requests ***")

            except Exception as e:
                LOGGER.exception(e)
      # Set the next thread to happen
        DATA_UPDATE_THREAD = threading.Timer(POOL_TIME_LIVE_LOADS, update_live_data, ())
        DATA_UPDATE_THREAD.start()

    def update_live_data():
        """
        Intraday update live loads based on POOL_TIME_LIVE_LOADS
        """
        global NEWLOAD_DF
        global DATA_UPDATE_THREAD
        global SCHEDULE_APPT

        with dataLock:
            # get live bazooka
            get_liveloads()  #
            NEWLOAD_DF = init_read_liveload('app_scheduler_live_loads.pkl')
            filename_APPT = 'app_scheduler_results{0}.pkl'.format(datetime.datetime.now().strftime('%Y-%m-%d'))
            SCHEDULE_APPT = init_read_preresults(filename_APPT)

            LOGGER.info("*** System Data Update ***")

        # Set the next thread to happen
        DATA_UPDATE_THREAD = threading.Timer(POOL_TIME_LIVE_LOADS, update_live_data, ())
        DATA_UPDATE_THREAD.start()

    # Initiate
    first_time_read()
    LOGGER.info('All files ready.')

    # When you kill Flask (SIGTERM), clear the trigger for the next thread
    atexit.register(interrupt)
    return app

app = create_app()


@app.route('/schedule_mimic/', methods=['GET'], strict_slashes=False)
def scheduler():
    global HISTLOAD_DF
    global FACILITY_HOUR_DF
    global NEWLOAD_DF
    global SCHEDULE_APPT

    try:
        LOGGER.info("Start to Process for api at time {0}".format(datetime.datetime.now()))
        values = CaseInsensitiveDict(request.values.to_dict())
        loadID = values.get('loadid', type=int, default=0)

        result_json, status = schedule_mimic(newloads_df=NEWLOAD_DF, histloads_df=HISTLOAD_DF,
                                             facility_hour_df=FACILITY_HOUR_DF, loadID=loadID,
                                             pre_results=SCHEDULE_APPT)

        LOGGER.info("END: Mimic Scheduling Process")

        LOGGER.info("Finish to Process for api at time {0}".format(datetime.datetime.now()))

        return jsonify({'Loads': result_json, "Version": CONFIG.API_VERSION, "Status": status})

    except Exception as ex:
        LOGGER.error(ex)


LOGGER.info("*** System Initialization ***")


if __name__ == '__main__':
    app.run(debug=True,port=5050, threaded=True, host='localhost')




