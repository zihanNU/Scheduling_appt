

def scheduler_spread(load_df):
    '''
    We had up to 3 options for each load.
    For those busy facility, we need to spread the appt time in same/different buckets
    '''

    features = ['LoadID', 'LoadDate', 'PU_Facility', 'DO_Facility', 'PU_Bucket', 'DO_Bucket', 'count', 'similarity', 'PU_Hour',
                 'DO_Hour', 'Transit', 'Dwell']  # for AD type
    pu_agg_df = load_df.groupby(['PU_Facility'])
    load_df.sort_values(by='PU_Facility')
