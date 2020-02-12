import logging

import datetime
import pytz

import config

LOGGER = logging.getLogger(__name__)
CONFIG = config.Config()

utc = pytz.utc
chicago_now = datetime.datetime.now(pytz.timezone(CONFIG.LOCALTIMEZONE))
offset_chicago=chicago_now.utcoffset().total_seconds() / 60 / 60


def offset(tz_name):
    """
    returns a location's time zone offset from Chicago in minutes.
    earlier than Chicago time will be negative, otherwise positive
    results in hours
    """
    try:
        city_now = datetime.datetime.now(pytz.timezone(tz_name))
        offset_city = city_now.utcoffset().total_seconds() / 60 / 60
        return (offset_chicago-offset_city)
    except:
        LOGGER.warn('time zone info not found for city with time zone {0}  '.format(tz_name))
        return 0