def feasibility_check(load_df, facility_df):
    Weekday_Mapper = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    load_df['DO_DOW'] = -1
    do_ind = load_df['DO_ScheduleType'].values == 1 & load_df['PU_Appt'].isna()
    pu_ind = load_df['PU_ScheduleType'].values == 1 & load_df['DO_Appt'].isna()
    load_df['pu_scheduletime'].fillna(load_df['PU_Appt'], inplace=True)
    load_df['do_scheduletime'].fillna(load_df['DO_Appt'], inplace=True)
    load_df['DO_DOW'] = load_df['do_scheduletime'].dt.dayofweek
    load_df['PU_DOW'] = load_df['pu_scheduletime'].dt.dayofweek
    load_df['PUopen'] = load_df.apply(lambda x: facility_df.loc[x['PU_Facility'], Weekday_Mapper[x['PU_DOW']]+'Open'])
    load_df['PUclose'] = load_df.apply(lambda x: facility_df.loc[x['PU_Facility'], Weekday_Mapper[x['PU_DOW']]+'Close'])
    load_df['DOopen'] = load_df.apply(lambda x: facility_df.loc[x['DO_Facility'], Weekday_Mapper[x['DO_DOW']]+'Open'])
    load_df['DOclose'] = load_df.apply(lambda x: facility_df.loc[x['DO_Facility'], Weekday_Mapper[x['DO_DOW']]+'Open'])



