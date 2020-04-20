import logging

# 3rd party
import pyodbc
import pandas as pd
import datetime

import config

CONFIG = config.Config()
LOGGER = logging.getLogger(__name__)


class QueryEngine():

    def __init__(self):
        self.connection_strings = {
            "DB research science":       CONFIG.CONNECT_STR_RS,
            "DB data science":           CONFIG.CONNECT_STR_DS,
            "DB live connection":        CONFIG.CONNECT_STR_LIVE,
        }

    def _df_from_sql_query(self, connection_string, sql_command):
        """ create a pandas dataframe from the given SQL command on the provided server

            if the connection string doesn't name one of the internal strings (refer to dict in __init__),
            it'll be used directly as the argument to pyodbc.connect()

            NOTE this may raise for at least
             - failed connections
             - failed SQL->DF conversion
        """
        LOGGER.info("generating pandas df from '{}'".format(sql_command))
        connection_string = self.connection_strings.get(connection_string, connection_string)
        with pyodbc.connect(connection_string) as cn:
            return pd.read_sql(sql_command, cn)

    def _sql_update(self, connection_string, sql_command):
        """ Execute a SQL command against the provided server

            if the connection string doesn't name one of the internal strings (refer to dict in __init__),
            it'll be used directly as the argument to pyodbc.connect()

            NOTE this may raise for at least
             - failed connections
             - commit failed
        """
        LOGGER.info("SQL update with '{}'".format(sql_command))
        connection_string = self.connection_strings.get(connection_string, connection_string)
        with pyodbc.connect(connection_string) as cn:
            cn.execute(sql_command)
            cn.commit()


    def daily_update_HistLoad(self):
        self._sql_update("DB research science", "EXEC [dbo].[uspScheduling_GetHistLoad]")
        LOGGER.info('Update historical load in Database Completed')

    def get_histload_initial(self):   # HERE we can consider last year same month data in the future
        sql_command = "EXEC [dbo].[uspScheduling_HistLoadtoModel] @StartDate = '{}'".format(
            datetime.date.today() - datetime.timedelta(days=CONFIG.HISTDATASTART),  # start date
        )
        return self._df_from_sql_query("DB research science", sql_command)

    def get_facility_initial(self):
        return self._df_from_sql_query("DB research science", "EXEC [dbo].[uspScheduling_FacilityHour]")

    def get_cityinfo_initial(self):
        return self._df_from_sql_query("DB research science", "EXEC [dbo].[uspScheduling_CityInfo]")

    def get_liveload(self):
        sql_command = "EXEC [dbo].[uspScheduling_GetLiveLoad] @LoadUpdateDateTime = '{}'".format(
            pd.Timestamp('today').to_period("D") # start date
        )
        return self._df_from_sql_query("DB research science", sql_command)

    def get_cluster_initial(self):
        return self._df_from_sql_query("DB data science", "EXEC [DataScience].[uspNOVA_ClusterInfo]")