import config
import pandas as pd
import logging
import os


LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()



def init_read_histload():
    try:
        hist_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'train_data_processed.csv'))
    except Exception as e:
        hist_data = pd.DataFrame()
        LOGGER.error("Cannot Find train_data File")
        LOGGER.exception(e)
    return hist_data



def init_read_liveload():
    try:
        live_data = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'test_data_processed_0408.csv'))
    except Exception as e:
        live_data = pd.DataFrame()
        LOGGER.error("Cannot Find test_data File")
        LOGGER.exception(e)
    return live_data

def init_read_facility():
    try:
        facility_hour = pd.read_csv(os.path.join(CONFIG.MODEL_PATH, 'facility_hour_0409.csv'))
        facility_hour.sort_values(by='FacilityID', inplace=True)
        facility_hour.set_index('FacilityID', drop=True, inplace=True)
    except Exception as e:
        facility_hour = pd.DataFrame()
        LOGGER.error("Cannot Find facility_hour File")
        LOGGER.exception(e)
    return facility_hour

