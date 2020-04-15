import pandas as pd
import numpy as np

weekday_mapper = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}


def feasibility_check(load_df, facility_df=pd.DataFrame(), ite=0):
    features = ['LoadID', 'Miles', 'LoadDate', 'PU_Facility', 'PU_ScheduleType', 'PU_Appt', 'pu_scheduletime',
                'DO_Facility', 'DO_ScheduleType', 'DO_Appt', 'do_scheduletime']
    #weekday_mapper = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    load_df['DO_DOW'] = -1
    do_ind = load_df['DO_ScheduleType'].values == 1 & load_df['DO_Appt'].isna()
    pu_ind = load_df['PU_ScheduleType'].values == 1 & load_df['PU_Appt'].isna()
    load_df['pu_scheduletime'].fillna(load_df['PU_Appt'], inplace=True)
    load_df['do_scheduletime'].fillna(load_df['DO_Appt'], inplace=True)
    load_df['DO_DOW'] = load_df['do_scheduletime'].dt.dayofweek
    load_df['PU_DOW'] = load_df['pu_scheduletime'].dt.dayofweek
    load_df['PU_Facility'] = load_df['PU_Facility'].astype(np.int32)
    load_df['DO_Facility'] = load_df['DO_Facility'].astype(np.int32)
    load_df['PU_Date'] = (load_df['pu_scheduletime']).dt.normalize()
    load_df['DO_Date'] = (load_df['do_scheduletime']).dt.normalize()
    if facility_df.shape[0] > 0:
        load_df = load_df.merge(facility_df, left_on='PU_Facility', right_index=True, how='left', copy=False)
        load_df['PUopen'] = 0.
        load_df['PUclose'] = 0.
        for k in weekday_mapper.keys():
            opencolname = weekday_mapper[k] +'Openhour'
            load_df[opencolname].fillna(np.float32(0), inplace=True)
            closecolname = weekday_mapper[k] +'Closehour'
            load_df[closecolname].fillna(np.float32(23.9999), inplace=True)
            pu_openind = (load_df['PU_DOW'] == k)
            pu_closeind = (load_df['PU_DOW'] == k)
            load_df.loc[pu_openind, 'PUopen'] = load_df.loc[pu_openind, opencolname]
            load_df.loc[pu_closeind, 'PUclose'] = load_df.loc[pu_closeind, closecolname]
        load_df.drop(columns=facility_df.columns, inplace=True)

        load_df = load_df.merge(facility_df, left_on='DO_Facility', right_index=True, how='left', copy=False)
        load_df['DOopen'] = 0.
        load_df['DOclose'] = 0.
        for k in weekday_mapper.keys():
            opencolname = weekday_mapper[k] +'Openhour'
            load_df[opencolname].fillna(np.float32(0), inplace=True)
            closecolname = weekday_mapper[k] +'Closehour'
            load_df[closecolname].fillna(np.float32(23.9999), inplace=True)
            do_openind = (load_df['DO_DOW'] == k)
            do_closeind = (load_df['DO_DOW'] == k)
            load_df.loc[do_openind, 'DOopen'] = load_df.loc[do_openind, opencolname]
            load_df.loc[do_closeind, 'DOclose'] = load_df.loc[do_closeind, closecolname]
        load_df.drop(columns=facility_df.columns, inplace=True)

    load_df['pu_scheduletime_check'] = load_df['pu_scheduletime'].values
    load_df['do_scheduletime_check'] = load_df['do_scheduletime'].values
    load_df['pudelta'] = 0
    load_df['dodelta'] = 0

    load_df['pu_schedulehour'] = pd.to_datetime(load_df['pu_scheduletime']).dt.hour
    pu_openind = pu_ind & (load_df['PUopen'].values > load_df['pu_schedulehour'].values + 0.01)  #not valid
    pu_closeind = pu_ind & (((load_df['PUclose'].values < load_df['pu_schedulehour'].values) &
                            (load_df['PUclose'].values > load_df['PUopen'].values))
                            | ((load_df['PUclose'].values > load_df['pu_schedulehour'].values) &
                            (load_df['PUclose'].values < load_df['PUopen'].values))
                            )
    pu_ind_check = pu_openind | pu_closeind

    if pu_openind.any() or pu_closeind.any():
        if pu_openind.any():
            load_df.loc[pu_openind, 'pu_schedulehour'] = load_df.loc[pu_openind, 'PUopen']
        if pu_closeind.any():
            load_df.loc[pu_closeind, 'pu_schedulehour'] = np.maximum(load_df.loc[do_closeind, 'PUopen'],
                                                                 load_df.loc[do_closeind, 'PUclose'])
        load_df.loc[pu_ind_check, 'pu_scheduletime_check'] = pd.to_datetime(load_df.loc[pu_ind_check, 'PU_Date']) +\
                                                     pd.to_timedelta(load_df.loc[pu_ind_check, 'pu_schedulehour'], unit='h')
        load_df.loc[pu_ind_check, 'pudelta'] = (pd.to_datetime(load_df.loc[pu_ind_check, 'pu_scheduletime_check']) -
                                                pd.to_datetime(load_df.loc[pu_ind_check, 'pu_scheduletime'])) / \
                                               pd.to_timedelta(1, unit='h')
        load_df.loc[pu_ind_check, 'do_scheduletime_check'] = pd.to_datetime(load_df.loc[pu_ind_check, 'do_scheduletime']).values + \
                                          pd.to_timedelta(load_df.loc[pu_ind_check, 'pudelta'], unit='h')

    load_df['do_schedulehour'] = pd.to_datetime(load_df['do_scheduletime']).dt.hour
    do_openind = do_ind & (load_df['DOopen'].values > load_df['do_schedulehour'].values)
    do_closeind = do_ind & (((load_df['DOclose'].values < load_df['do_schedulehour'].values) &
                             (load_df['DOclose'].values > load_df['DOopen'].values))
                            | ((load_df['DOclose'].values > load_df['do_schedulehour'].values) &
                            (load_df['DOclose'].values < load_df['DOopen'].values))
                            )
    do_ind_check = do_openind | do_closeind

    if do_openind.any() or do_closeind.any():
        if do_openind.any():
            load_df.loc[do_openind, 'do_schedulehour'] = load_df.loc[do_openind, 'DOopen']
        if do_closeind.any():
            load_df.loc[do_closeind, 'do_schedulehour'] = np.maximum(load_df.loc[do_closeind, 'DOopen'],
                                                                 load_df.loc[do_closeind, 'DOclose'])
        load_df.loc[do_ind_check, 'do_scheduletime_check'] = pd.to_datetime(load_df.loc[do_ind_check, 'DO_Date']) + \
                                                     pd.to_timedelta(load_df.loc[do_ind_check, 'do_schedulehour'],
                                                                     unit='h')
        load_df.loc[do_ind_check, 'dodelta'] = (pd.to_datetime(load_df.loc[do_ind_check, 'do_scheduletime_check']) -
                                                pd.to_datetime(load_df.loc[do_ind_check, 'do_scheduletime'])) / \
                                               pd.to_timedelta(1, unit='h')
        load_df.loc[do_ind_check, 'pu_scheduletime_check'] = pd.to_datetime(
            load_df.loc[do_ind_check, 'pu_scheduletime_check']).values + pd.to_timedelta(load_df.loc[do_ind_check, 'dodelta'],
                                                                             unit='h')
        load_df['PU_Date'] = pd.to_datetime(load_df['pu_scheduletime_check']).dt.normalize()
        pu_date_check = (pd.to_datetime(load_df['PU_Date']) - pd.to_datetime(load_df['LoadDate'])) / pd.to_timedelta(1, unit='h') < 0
        if pu_date_check.any():
            load_df['pu_scheduletime_check'] = pd.to_datetime(load_df['pu_scheduletime_check']) + pd.to_timedelta(1, unit='day')
            load_df['do_scheduletime_check'] = pd.to_datetime(load_df['do_scheduletime_check']) + pd.to_timedelta(1, unit='day')

    if (pu_ind_check.any() or do_ind_check.any()):
        load_df['pu_scheduletime'] = load_df['pu_scheduletime_check'].copy()
        load_df['do_scheduletime'] = load_df['do_scheduletime_check'].copy()
        load_df = dup_check(load_df)

    if (pu_ind_check.any() or do_ind_check.any()) and ite < 5:
        load_df = feasibility_check(load_df, ite=ite+1)

    return load_df[features]


def dup_check(df):
    dup_doind = df.groupby(['DO_Date', 'DO_Facility', 'do_schedulehour']).cumcount()
    do_ind = df['DO_ScheduleType'] == 1
    df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] + 0.5
    df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] + 0.25
    df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] - 0.25

    df['do_scheduletime'] = pd.to_datetime(df['DO_Date']) + pd.to_timedelta(df['do_schedulehour'], unit='h')

    dup_puind = df.groupby(['PU_Date', 'PU_Facility', 'pu_schedulehour']).cumcount()
    pu_ind = df['PU_ScheduleType'] == 1

    df.loc[dup_puind == 1 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 1 & pu_ind, 'pu_schedulehour'] + 0.5
    df.loc[dup_puind == 2 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 2 & pu_ind, 'pu_schedulehour'] + 0.25
    df.loc[dup_puind == 3 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 3 & pu_ind, 'pu_schedulehour'] - 0.25

    df['pu_scheduletime'] = pd.to_datetime(df['PU_Date']) + pd.to_timedelta(df['pu_schedulehour'], unit='h')
    return df

