import numpy as np
import pandas as pd
import logging


LOGGER = logging.getLogger(__name__)

def scheduler_spread(df):
    '''
    We had up to 3 options for each load.
    For those busy facility, we need to spread the appt time in same/different buckets

    We need to set the date first. The pu date is loaddate, and do date needs to verify with the travel time
    '''
    LOGGER.info("start spread the hours for facility")
    speed_base = 45
    buffer = 1
    features = ['LoadID', 'Miles',  'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime', 'PUOffset', 'DOOffset',
                'transit', 'dwelltime']

    bucket = {'0-0': 0, '0-1': 0.5, '0-2': 1.0, '0-3': 1.5, '0-4': 2.0, '0-5': 2.5, '0-6': 3.0, '0-7': 3.5, '0-8': 4.0,
              '0-9': 4.5, '0-10': 5, '0-11': 5.5, '0-12': 0, '0-13': 0.5, '0-14': 1.0, '0-15': 1.5, '0-16': 2.0,
              '0-17': 2.5, '0-18': 3.0, '0-19': 3.5, '0-20': 4.0, '0-21': 4.5, '0-22': 5, '0-23': 5.5, '0-24': 0,
              '0-25': 0.5, '0-26': 1.0, '0-27': 1.5, '0-28': 2.0, '0-29': 2.5, '0-30': 3.0,

              '10-0': 8.0, '10-1': 8.5, '10-2': 9.0, '10-3': 9.5, '10-4': 7.0, '10-5': 6.5, '10-6': 6.0, '10-7': 7.5,
              '10-8': 8.25, '10-9': 8.75, '10-10': 9.25, '10-11': 9.75, '10-12': 7.25, '10-13': 7.75,
              '10-14': 6.25, '10-15': 6.75, '10-16': 8.0, '10-17': 8.5, '10-18': 9.0, '10-19': 9.5, '10-20': 7.0,
              '10-21': 6.5, '10-22': 6.0, '10-23': 7.5, '10-24': 8.25, '10-25': 8.75, '10-26': 9.25, '10-27': 9.75,
              '10-28': 7.25, '10-29': 7.75, '10-30': 6.75,

              '20-0': 10, '20-1': 10.5, '20-2': 11.0, '20-3': 11.5, '20-4': 12.0, '20-5': 12.5, '20-6': 13.0,
              '20-7': 10.25, '20-8': 10.75, '20-9': 11.25, '20-10': 11.75, '20-11': 12.25, '20-12': 12.75,
              '20-13': 10, '20-14': 10.5, '20-15': 11.0, '20-16': 11.5, '20-17': 12.0, '20-18': 12.5, '20-19': 13.0,
              '20-20': 10.25, '20-21': 10.75, '20-22': 11.25, '20-23': 11.75, '20-24': 12.25, '20-25': 12.75,
              '20-26': 10.25, '20-27': 10.75, '20-28': 11.25, '20-29': 11.75, '20-30': 12.25,

              '30-0': 13, '30-1': 13.5, '30-2': 14.0, '30-3': 14.5, '30-4': 15.0, '30-5': 15.5, '30-6': 16,
              '30-7': 16.5, '30-8': 13.25, '30-9': 13.75, '30-10': 14.25, '30-11': 14.75, '30-12': 15.25,
              '30-13': 15.75, '30-14': 16.25, '30-15': 16.75,
              '30-16': 13, '30-17': 13.5, '30-18': 14.0, '30-19': 14.5, '30-20': 15.0, '30-21': 15.5, '30-22': 16,
              '30-23': 16.5, '30-24': 13.25, '30-25': 13.75, '30-26': 14.25, '30-27': 14.75, '30-28': 15.25,
              '30-29': 15.75, '30-30': 16.25,

              '40-0': 17.0, '40-1': 17.5, '40-2': 18.0, '40-3': 18.5, '40-4': 19.0, '40-5': 19.5, '40-6': 20.0,
              '40-7': 20.5, '40-8': 21.0, '40-9': 21.5, '40-10': 17.25, '40-11': 17.75, '40-12': 18.25,
              '40-13': 18.75, '40-14': 19.25, '40-15': 19.75, '40-16': 20.25, '40-17': 20.75,
              '40-18': 17.0, '40-19': 17.5, '40-20': 18.0, '40-21': 18.5, '40-22': 19.0, '40-23': 19.5, '40-24': 20.0,
              '40-25': 20.5, '40-26': 21.0, '40-27': 21.5, '40-28': 17.25, '40-29': 17.75, '40-30': 18.25,

              '50-0': 22.0, '50-1': 22.5, '50-2': 21.0, '50-3': 21.5, '50-4': 23.0, '50-5': 23.5,
              '50-6': 22.25, '50-7': 22.75, '50-8': 21.25, '50-9': 21.75, '50-10': 23.25, '50-11': 23.75,
              '50-12': 22.0, '50-13': 22.5, '50-14': 21.0, '50-15': 21.5, '50-16': 23.0, '50-17': 23.5,
              '50-18': 22.25, '50-19': 22.75, '50-20': 21.25, '50-21': 21.75, '50-22': 23.25, '50-23': 23.75,
              '50-24': 22.25, '50-25': 22.75, '50-26': 21.25, '50-27': 21.75, '50-28': 23.25, '50-29': 23.75,
              '50-30': 22.0,
              }
    df_rank_pu = df.groupby(['LoadDate', 'PU_Facility', 'PU_Bucket']).cumcount()
    df_rank_do = df.groupby(['LoadDate', 'DO_Facility', 'DO_Bucket']).cumcount()
    df['dwell_est'] = 2
    df['travel_est'] = df['Miles'].values / speed_base
    df['resttime_est'] = np.int32(df['travel_est'].values / 10) * 10 + np.int32(df['travel_est'].values / 4)
    df['transit_est'] = df['travel_est'] + df['resttime_est']
    df['pu_ranking'] = df_rank_pu
    df['do_ranking'] = df_rank_do
    pu_sche_ind = (df['PU_ScheduleType'] == 1) & (df['PU_Appt'].isna())
    do_sche_ind = (df['DO_ScheduleType'] == 1) & (df['DO_Appt'].isna())

    pu_schedule_max = df.groupby(['LoadDate', 'PU_Facility', 'PU_Bucket']).agg({'pu_ranking': 'max'}).rename(
        columns={'pu_ranking': 'pu_ranking_max'})
    do_schedule_max = df.groupby(['LoadDate', 'DO_Facility', 'DO_Bucket']).agg({'do_ranking': 'max'}).rename(
        columns={'do_ranking': 'do_ranking_max'})
    df = df.merge(pu_schedule_max, on=['LoadDate', 'PU_Facility', 'PU_Bucket'], how='left')
    df = df.merge(do_schedule_max, on=['LoadDate', 'DO_Facility', 'DO_Bucket'], how='left')
    df['pu_schedulehour'] = np.nan
    df['do_schedulehour'] = np.nan
    df.loc[pd.notna(df['PU_Appt']), 'pu_schedulehour'] = pd.to_datetime(df.loc[pd.notna(df['PU_Appt']),'PU_Appt']).dt.hour
    df.loc[pd.notna(df['DO_Appt']), 'do_schedulehour'] = pd.to_datetime(df.loc[pd.notna(df['PU_Appt']),'PU_Appt']).dt.hour

    pu_max_ind = df['pu_ranking_max'].values == 0
    do_max_ind = df['do_ranking_max'].values == 0

    pu_overrank = (df['pu_ranking'].values - df['pu_ranking_max'].values) > 0
    do_overrank = (df['do_ranking'].values - df['do_ranking_max'].values) > 0

    if pu_overrank.any():
        df.loc[pu_overrank, 'pu_ranking'] = df.loc[pu_overrank, 'pu_ranking'] - df.loc[pu_overrank, 'pu_ranking_max']
    if do_overrank.any():
        df.loc[do_overrank, 'pu_ranking'] = df.loc[do_overrank, 'pu_ranking'] - df.loc[do_overrank, 'pu_ranking_max']


    df.loc[pu_max_ind & pu_sche_ind, 'pu_schedulehour'] = np.int32(df.loc[pu_max_ind & pu_sche_ind, 'PU_Hour']/0.5)*0.5
    df.loc[do_max_ind & do_sche_ind, 'do_schedulehour'] = np.int32(df.loc[do_max_ind & do_sche_ind, 'DO_Hour']/0.5)*0.5

    df.loc[(~pu_max_ind) & pu_sche_ind, 'pu_schedulehour'] = df.loc[(~pu_max_ind) & pu_sche_ind].apply(lambda x:
                                                               bucket[str(x['PU_Bucket']) + '-' + str(x['pu_ranking'])], axis=1)
    df.loc[(~do_max_ind) & do_sche_ind, 'do_schedulehour'] = df.loc[(~do_max_ind) & do_sche_ind].apply(lambda x:
                                                               bucket[str(x['DO_Bucket']) + '-' + str(x['do_ranking'])], axis=1)

    # fillna for those puappt is na but type 2 or 3, and doappt is type 1 and na.
    pu_na_ind = df['pu_schedulehour'].isna()
    df.loc[pu_na_ind, 'pu_schedulehour'] = np.int32(df.loc[pu_na_ind, 'PU_Hour'] / 0.5) * 0.5
    do_na_ind = df['do_schedulehour'].isna()
    df.loc[do_na_ind, 'do_schedulehour'] = np.int32(df.loc[do_na_ind, 'DO_Hour'] / 0.5) * 0.5

    df['pu_scheduletime'] = pd.to_datetime(df['LoadDate']) + pd.to_timedelta(df['pu_schedulehour'], unit='h')

    df['DO_Appt_est'] = df['pu_scheduletime'] + pd.to_timedelta((df['transit_est'] + df['dwell_est']
                                                                 - df['PUOffset'].values + df['DOOffset'].values
                                                                 + buffer), unit='h')
    df['DO_Date'] = df['DO_Appt_est'].dt.normalize()
    df['DO_hour_est'] = df['DO_Appt_est'].dt.hour
    do_diffind = (np.abs(df['do_schedulehour'] - df['DO_hour_est']) >= 3) & (df['count'] < 3)
    df.loc[do_diffind & do_sche_ind, 'do_schedulehour'] = np.int32(df.loc[do_diffind & do_sche_ind, 'DO_hour_est']/0.5) * 0.5

    # calculate if the reset make the hour eariler and no enough travel time there, set the date to date + 1
    df['do_scheduletime'] = pd.to_datetime(df['DO_Date']) + pd.to_timedelta(df['do_schedulehour'], unit='h')
    df['travel_reset'] = (pd.to_datetime(df['do_scheduletime']) - pd.to_datetime(df['pu_scheduletime']))/pd.to_timedelta(1, unit='h')
    travel_ind = df['travel_reset'] < df['travel_est']
    df.loc[travel_ind & do_sche_ind, 'DO_Date'] = df.loc[travel_ind & do_sche_ind, 'DO_Date'] + pd.to_timedelta(1, unit='day')

# check dup in spread hour
    dup_puind = df.groupby(['LoadDate', 'PU_Facility', 'pu_schedulehour']).cumcount()
    if dup_puind.max():
        df.loc[(dup_puind == 1) & pu_sche_ind, 'pu_schedulehour'] = df.loc[(dup_puind == 1) & pu_sche_ind, 'pu_schedulehour'] + 0.5
        df.loc[(dup_puind == 2) & pu_sche_ind, 'pu_schedulehour'] = df.loc[(dup_puind == 2) & pu_sche_ind, 'pu_schedulehour'] - 0.5
        df.loc[(dup_puind == 3) & pu_sche_ind, 'pu_schedulehour'] = df.loc[(dup_puind == 3) & pu_sche_ind, 'pu_schedulehour'] + 0.25
        df.loc[(dup_puind == 4) & pu_sche_ind, 'do_schedulehour'] = df.loc[(dup_puind == 4) & pu_sche_ind, 'pu_schedulehour'] + 0.75
    df['pu_scheduletime'] = pd.to_datetime(df['LoadDate']) + pd.to_timedelta(df['pu_schedulehour'], unit='h')

    dup_doind = df.groupby(['DO_Date', 'DO_Facility', 'do_schedulehour']).cumcount()

    if dup_doind.max():
        df.loc[(dup_doind == 1) & do_sche_ind, 'do_schedulehour'] = df.loc[(dup_doind == 1) & do_sche_ind, 'do_schedulehour'] + 0.5
        df.loc[(dup_doind == 2) & do_sche_ind, 'do_schedulehour'] = df.loc[(dup_doind == 2) & do_sche_ind, 'do_schedulehour'] - 0.5
        df.loc[(dup_doind == 3) & do_sche_ind, 'do_schedulehour'] = df.loc[(dup_doind == 3) & do_sche_ind, 'do_schedulehour'] + 0.25
        df.loc[(dup_doind == 4) & do_sche_ind, 'do_schedulehour'] = df.loc[(dup_doind == 4) & do_sche_ind, 'do_schedulehour'] + 0.75
    df['do_scheduletime'] = pd.to_datetime(df['DO_Date']) + pd.to_timedelta(df['do_schedulehour'], unit='h')
    df.rename(columns={'Transit': 'transit', 'Dwell': 'dwelltime'}, inplace=True)
    LOGGER.info("spreading hours done")
    return df[features]
