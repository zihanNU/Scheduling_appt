import pandas as pd
import numpy as np

weekday_mapper = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}


def feasibility_check(load_df, facility_df):
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
    load_df = load_df.merge(facility_df, left_on='PU_Facility', right_index=True, how='left', copy=False)
    t = load_df.loc[0]
    load_df['PUopen'] = 0.
    load_df['PUclose'] = 0.
    load_df['DOopen'] = 0.
    load_df['DOclose'] = 0.
    for k in weekday_mapper.keys():
        opencolname = weekday_mapper[k] +'Openhour'
        load_df[opencolname].fillna(np.float32(0), inplace=True)
        closecolname = weekday_mapper[k] +'Closehour'
        load_df[closecolname].fillna(np.float32(23.99), inplace=True)
        pu_openind = (load_df['PUopen'] == k)
        pu_closeind = (load_df['PUclose'] == k)
        do_openind = (load_df['DOopen'] == k)
        do_closeind = (load_df['DOclose'] == k)
        load_df.loc[pu_openind, 'PUopen'] = load_df.loc[pu_openind, opencolname]
        load_df.loc[do_openind, 'DOopen'] = load_df.loc[do_openind, opencolname]
        load_df.loc[pu_closeind, 'PUclose'] = load_df.loc[pu_closeind, opencolname]
        load_df.loc[do_closeind, 'DOclose'] = load_df.loc[do_closeind, opencolname]

    load_df.drop(columns=facility_df.columns, inplace=True)

    load_df['pu_schedulehour'] = pd.to_datetime(load_df['pu_scheduletime']).dt.hour
    pu_openind = pu_ind & (load_df['PUopen'].values > load_df['pu_schedulehour'].values)  #not valid
    pu_closeind = pu_ind & (load_df['PUclose'].values < load_df['pu_schedulehour'].values)

    buffer = 1
    if pu_openind.any() or pu_closeind.any():
        if pu_openind.any():
            load_df.loc[pu_openind, 'pu_schedulehour'] = load_df.loc[pu_openind, 'PUopen']
        else:
            load_df.loc[pu_openind, 'pu_schedulehour'] = load_df.loc[pu_openind, 'PUclose']
        load_df.loc[pu_openind, 'pu_scheduletime'] = pd.to_datetime(load_df.loc[pu_openind, 'LoadDate']) +\
                                                     pd.to_timedelta(load_df.loc[pu_openind, 'pu_schedulehour'], unit='h')
        load_df.loc[pu_openind, 'do_scheduletime'] = pd.to_datetime(load_df.loc[pu_openind, 'pu_scheduletime']).values + \
                                          pd.to_timedelta((load_df.loc[pu_openind, 'transit'].values +
                                                           load_df.loc[pu_openind, 'dwelltime'].values -
                                                           load_df.loc[pu_openind, 'PUOffset'].values +
                                                           load_df.loc[pu_openind, 'DOOffset'].values + buffer), unit='h')

    load_df['do_schedulehour'] = pd.to_datetime(load_df['do_scheduletime']).dt.hour
    do_openind = do_ind & (load_df['DOopen'].values > load_df['do_schedulehour'].values)
    do_closeind = do_ind & (load_df['DOclose'].values < load_df['do_schedulehour'].values)
    if do_openind.any() or do_closeind.any():
        load_df['DO_Date'] = (load_df['do_scheduletime']).dt.normalize()
        if do_openind.any():
            load_df.loc[do_openind, 'do_schedulehour'] = load_df.loc[do_openind, 'DOopen']
        else:
            load_df.loc[do_openind, 'do_schedulehour'] = load_df.loc[do_openind, 'DOopen']
            load_df.loc['DO_Date'] = load_df.loc['DO_Date'] + pd.to_timedelta(1, unit='day')
        load_df.loc[do_openind, 'do_scheduletime'] = pd.to_datetime(load_df.loc[do_openind, 'DO_Date']) + \
                                                     pd.to_timedelta(load_df.loc[do_openind, 'do_schedulehour'],
                                                                     unit='h')
    load_df = dup_check(load_df)

    return load_df[features]


def dup_check(df):
    dup_doind = df.groupby(['DO_Date', 'DO_Facility', 'do_schedulehour']).cumcount()
    do_ind = df['DO_ScheduleType'] == 1
    df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 1 & do_ind, 'do_schedulehour'] + 0.5
    df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 2 & do_ind, 'do_schedulehour'] + 0.25
    df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] = df.loc[dup_doind == 3 & do_ind, 'do_schedulehour'] - 0.25

    df['do_scheduletime'] = pd.to_datetime(df['DO_Date']) + pd.to_timedelta(df['do_schedulehour'], unit='h')

    dup_puind = df.groupby(['Load_Date', 'PU_Facility', 'pu_schedulehour']).cumcount()
    pu_ind = df['PU_ScheduleType'] == 1

    df.loc[dup_puind == 1 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 1 & pu_ind, 'pu_schedulehour'] + 0.5
    df.loc[dup_puind == 2 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 2 & pu_ind, 'pu_schedulehour'] + 0.25
    df.loc[dup_puind == 3 & pu_ind, 'pu_schedulehour'] = df.loc[dup_puind == 3 & pu_ind, 'pu_schedulehour'] - 0.25

    df['pu_scheduletime'] = pd.to_datetime(df['Load_Date']) + pd.to_timedelta(df['pu_schedulehour'], unit='h')
    return df


def locate(row):
    w_ind = np.tuple(row['PU_DOW'])
    colname = weekday_mapper[w_ind] + 'Openhour'
    load_df['PUopen'] = load_df.apply(lambda x: x[weekday_mapper[x['PU_DOW']] + 'Openhour'])
    load_df['PUclose'] = load_df.apply(lambda x: x[weekday_mapper[x['PU_DOW']] + 'Closehour'])
    load_df['DOopen'] = load_df.apply(lambda x: x[weekday_mapper[x['DO_DOW']] + 'Openhour'])
    load_df['DOclose'] = load_df.apply(lambda x: x[weekday_mapper[x['DO_DOW']] + 'Openhour'])
    # load_df['PUopen'] = load_df.apply(lambda x:  facility_df.loc[x['PU_Facility'], weekday_mapper[x['PU_DOW']]+'Openhour'])
    # load_df['PUclose'] = load_df.apply(lambda x: facility_df.loc[x['PU_Facility'], weekday_mapper[x['PU_DOW']]+'Closehour'])
    # load_df['DOopen'] = load_df.apply(lambda x: facility_df.loc[x['DO_Facility'], weekday_mapper[x['DO_DOW']]+'Openhour'])
    # load_df['DOclose'] = load_df.apply(lambda x: facility_df.loc[x['DO_Facility'], weekday_mapper[x['DO_DOW']]+'Openhour'])

    return row[colname]

