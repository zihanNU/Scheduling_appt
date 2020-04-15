import numpy as np
import pandas as pd


def scheduler_spread(df):
    '''
    We had up to 3 options for each load.
    For those busy facility, we need to spread the appt time in same/different buckets
    '''

    features = ['LoadID', 'Miles',  'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime', 'PUOffset', 'DOOffset',
                'transit', 'dwelltime']

    bucket = {'0-0': 0, '0-1': 0.5, '0-2': 1.0, '0-3': 1.5, '0-4': 2.0, '0-5': 2.5, '0-6': 3.0, '0-7': 3.5, '0-8': 4.0,
              '0-9': 4.5, '0-10': 5, '0-11': 5.5,
              '10-0': 8.0, '10-1': 8.5, '10-2': 9.0, '10-3': 9.5, '10-4': 7.0, '10-5': 6.5, '10-6': 6.0, '10-7': 7.5,
              '10-8': 8.25, '10-9': 8.75, '10-10': 9.25, '10-11': 9.75, '10-12': 7.25, '10-13': 7.75,
              '10-14': 6.25, '10-15': 6.75,
              '20-0': 10, '20-1': 10.5, '20-2': 11.0, '20-3': 11.5, '20-4': 12.0, '20-5': 12.5, '20-6': 13.0,
              '20-7': 10.25, '20-8': 10.75, '20-9': 11.25, '20-10': 11.75, '20-11': 12.25, '20-12': 12.75,
              '30-0': 13, '30-1': 13.5, '30-2': 14.0, '30-3': 14.5, '30-4': 15.0, '30-5': 15.5, '30-6': 16,
              '30-7': 16.5, '30-8': 13.25, '30-9': 13.75, '30-10': 14.25, '30-11': 14.75, '30-12': 15.25,
              '30-13': 15.75, '30-14': 16.25, '30-15': 16.75,
              '40-0': 17.0, '40-1': 17.5, '40-2': 18.0, '40-3': 18.5, '40-4': 19.0, '40-5': 19.5, '40-6': 20.0,
              '40-7': 20.5, '40-8': 21.0, '40-9': 21.5, '40-10': 17.25, '40-11': 17.75, '40-12': 18.25,
              '40-13': 18.75, '40-14': 19.25, '40-15': 19.75, '40-16': 20.25, '40-17': 20.75,
              '50-0': 22.0, '50-1': 22.5, '50-2': 23, '50-3': 21.5, '50-4': 23.0, '50-5': 23.5
              }
    buffer = 1

    df['PU_Date'] = (df['pu_scheduletime']).dt.normalize()
    df['DO_Date'] = (df['do_scheduletime']).dt.normalize()
    df_rank_pu = df.groupby(['LoadDate', 'PU_Facility', 'PU_Bucket']).cumcount()
    df_rank_do = df.groupby(['LoadDate', 'DO_Facility', 'DO_Bucket']).cumcount()

    df['pu_ranking'] = df_rank_pu
    df['do_ranking'] = df_rank_do
    pu_schedule_max = df.groupby(['LoadDate', 'PU_Facility', 'PU_Bucket']).agg({'pu_ranking': 'max'}).rename(
        columns={'pu_ranking': 'pu_ranking_max'})
    do_schedule_max = df.groupby(['LoadDate', 'DO_Facility', 'DO_Bucket']).agg({'do_ranking': 'max'}).rename(
        columns={'do_ranking': 'do_ranking_max'})
    df = df.merge(pu_schedule_max, on=['LoadDate', 'PU_Facility', 'PU_Bucket'], how='left')
    df = df.merge(do_schedule_max, on=['LoadDate', 'DO_Facility', 'DO_Bucket'], how='left')
    df['pu_schedulehour'] = np.nan
    df['do_schedulehour'] = np.nan

    pu_ind = df['pu_ranking_max'].values == 0
    do_ind = df['do_ranking_max'].values == 0

    df.loc[pu_ind, 'pu_schedulehour'] = np.int32(df.loc[pu_ind, 'PU_Hour'])
    df.loc[do_ind, 'do_schedulehour'] = np.int32(df.loc[do_ind, 'DO_Hour'])

    df.loc[~pu_ind, 'pu_schedulehour'] = df.loc[~pu_ind].apply(lambda x:
                                                               bucket[str(x['PU_Bucket']) + '-' + str(x['pu_ranking'])], axis=1)
    df.loc[~do_ind, 'do_schedulehour'] = df.loc[~do_ind].apply(lambda x:
                                                               bucket[str(x['DO_Bucket']) + '-' + str(x['do_ranking'])], axis=1)

    df['pu_scheduletime'] = pd.to_datetime(df['PU_Date']) + pd.to_timedelta(df['pu_schedulehour'], unit='h')
    df['DO_Appt_est'] = df['pu_scheduletime'] + pd.to_timedelta((df['Transit'] + df['Dwell']
                                                                 - df['PUOffset'].values + df['DOOffset'].values
                                                                 + buffer), unit='h')

    df['DO_Date'] = df['DO_Appt_est'].dt.normalize()

    df['DO_hour_est'] = df['DO_Appt_est'].dt.hour

    do_diffind = (np.abs(df['do_schedulehour'] - df['DO_hour_est']) >= 3) & (df['count'] < 3)
    df.loc[do_diffind, 'do_schedulehour'] = df.loc[do_diffind, 'DO_hour_est']

    dup_doind = df.groupby(['DO_Date', 'DO_Facility', 'do_schedulehour']).cumcount()
    do_ind = df['DO_ScheduleType'] == 1

    df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] + 0.5
    df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] - 0.5
    df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] + 0.25
    df.loc[dup_doind == 4 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 4 & do_ind, 'do_schedulehour'] + 0.75
    df['do_scheduletime'] = pd.to_datetime(df['DO_Date']) + pd.to_timedelta(df['do_schedulehour'], unit='h')
    df.rename(columns={'Transit':'transit', 'Dwell': 'dwelltime'}, inplace=True)
    return df[features]
