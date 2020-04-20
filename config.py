"""Please make sure computer have environment of APP_SETTINGS
If not, in CMD: set APP_SETTINGS=config.XXXConfig
XXX is either Production/Local/Development/Testing
"""
import datetime
import json
import os
import logging
import logging.config
import platform
import xml.etree.ElementTree
import lxml.etree as etree
import io

import numpy as np

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_FILE = os.path.join(DIR_PATH, 'Scheduling_Rule.Api.config')
VERSION_FILE = os.path.join(DIR_PATH, 'Scheduling_Rule.Api.version')

try:
    root = xml.etree.ElementTree.parse(CONFIG_FILE).getroot()
    node = etree.parse(CONFIG_FILE)
except Exception as e:
    #logging not configed yet
    print('error in xml.etree.ElementTree.parse({}).getroot()'.format(CONFIG_FILE))
    ## TODO assign the root
    raise e

class Config(object):
    DEBUG = False
    TESTING = False
    CONNECT_STR = node.xpath('//add[@name="BazookaConnectionString"]')[0].attrib['connectionString']
    CONNECT_STR_LIVE = node.xpath('//add[@name="BazookaLiveConnectionString"]')[0].attrib['connectionString']
    CONNECT_STR_RS = node.xpath('//add[@name="ResearchScienceConnectionString"]')[0].attrib['connectionString']
    CONNECT_STR_ANLY = node.xpath('//add[@name="AnalyticsConnectionString"]')[0].attrib['connectionString']



    _model_path = node.xpath('//add[@key="modelPath"]')[0].attrib['value']
    _model_local_path = node.xpath('//add[@key="modelLocalPath"]')[0].attrib['value']
    _log_path = node.xpath('//add[@key="logPath"]')[0].attrib['value']
    API_MODEL_VERSION = node.xpath('//add[@key="apiModelVersion"]')[0].attrib['value']

    HISTDATASTART = int(node.xpath('//add[@key="histDataStart"]')[0].attrib['value'])
    # the start days (going back) to find the historical load and search history for carrier
    LOCALTIMEZONE = node.xpath('//add[@key="localTimeZone"]')[0].attrib['value']
    SIMILARITY_REF = float(node.xpath('//add[@key="similarityRef"]')[0].attrib['value'])
    WEIGHTSCALE = int(node.xpath('//add[@key="weightScale"]')[0].attrib['value'])
    MILESCALE = int(node.xpath('//add[@key="mileScale"]')[0].attrib['value'])
    OD_DIST_SCALE = int(node.xpath('//add[@key="odDistScale"]')[0].attrib['value'])


    _live_update_start_time = node.xpath(
        '//add[@key="liveUpdateStartTime"]')[0].attrib['value']
    _live_update_end_time = node.xpath(
        '//add[@key="liveUpdateEndTime"]')[0].attrib['value']


    _model_path = '' if _model_path is None else _model_path
    _model_local_path = '' if _model_local_path is None else _model_local_path
    _log_path = '' if _log_path is None else _log_path

    replace_arg = ("/", "\\")
    if platform.system() != "Windows":
        replace_arg = ("", "")

    MODEL_PATH = _model_path if os.path.isabs(_model_path) \
        else os.path.join(DIR_PATH, _model_path.replace(*replace_arg))
    LOCAL_MODEL_PATH = _model_local_path if os.path.isabs(_model_local_path) \
        else os.path.join(DIR_PATH, _model_local_path.replace(*replace_arg))
    LOG_PATH = _log_path if os.path.isabs(_log_path) \
        else os.path.join(DIR_PATH, _log_path.replace(*replace_arg))


    for path in (LOG_PATH, MODEL_PATH, LOCAL_MODEL_PATH):
        os.makedirs(path, exist_ok=True)

    with open("logging.json", 'r') as f:
        LOGGING_CONFIG = json.load(f)
    FNAME = 'Scheduling.'+ datetime.datetime.now().strftime('%Y-%m-%d')+'.log'
    LOGGING_CONFIG['handlers']['file_handler']['filename'] = os.path.join(LOG_PATH, FNAME)

    logging.config.dictConfig(LOGGING_CONFIG)
    LOGGER = logging.getLogger(__name__)

    HOST_NAME = node.xpath('//add[@key="ServerName"]')[0].attrib['value']
    PORT_NUM = int(node.xpath('//add[@key="ServerPort"]')[0].attrib['value'])

    # get live update start / end times
    try:
        LIVE_UPDATE_START_TIME = datetime.datetime.strptime(
            _live_update_start_time, "%H:%M:%S").time()
    except Exception as e:
        LOGGER.warning("Not able to convert live_update_start_time to time()")
        LIVE_UPDATE_START_TIME = datetime.time(6, 0, 0)
    try:
        LIVE_UPDATE_END_TIME = datetime.datetime.strptime(
            _live_update_end_time, "%H:%M:%S").time()
    except Exception as e:
        LOGGER.warning("Not able to convert live_update_start_time to time()")
        LIVE_UPDATE_END_TIME = datetime.time(22, 0, 0)

    _updates = root.find('updates')
    # update every 5min(300sec) for intraday live updates
    LIVE_LOAD_POOL_TIME = _updates.find('liveLoadPoolTimeSec').text
    # delete older daily files to save disk
    FILE_STORAGE_DAYS = _updates.find('fileStorageDays').text
    # drop_duplicates on df_scores in OfferScore().getScores every 5 min
    LIVE_OFFERS_DROP_SEC = int(_updates.find('liveOffersDropSec').text)
    # Every 3 hours write to cache
    OFFER_CACHE_INTERVAL = int(_updates.find('offerCacheWriteHours').text)
    ENABLE_QUERIES = int(_updates.find('enableQueries').text)

    try:
        f = io.open(VERSION_FILE, 'r', encoding='utf-16-le')
        #API_VERSION = f.readline().strip().replace(u'\ufeff', '')
        API_VERSION = 'Scheduling_Mimic_Api_1.0.0'
        f.close()
    except Exception as e:
        LOGGER.error('Fail to get the version: {}'.format(e))
        API_VERSION = 'Scheduling_Mimic_Api_1.0.0'


try:
    CONFIG = Config()
except Exception as e:
    # logging not configed yet
    LOGGER.error('error in Config() {0}'.format(e))
    ## TODO make new CONFIG
    raise e
