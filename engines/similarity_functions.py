def similarity_cal_ml(df):
    df['ref'] = CONFIG.SIMILARITY_REF
    histload_feature = {'weight': df.hist_weight/CONFIG.WEIGHTSCALE,
                        'length': df.hist_miles/CONFIG.MILESCALE,
                        'pallet': df.hist_pallet / CONFIG.WEIGHTSCALE,
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'weight': df.new_weight/CONFIG.WEIGHTSCALE,
                       'length': df.new_miles/CONFIG.MILESCALE,
                       'pallet': df.new_pallet / CONFIG.WEIGHTSCALE,
                       }
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    facility_hour_ml_df = {'LoadID': df['LoadID'],
                           'histloadID': df['histloadID'],
                           'similarity': sim_row,
                           'PU_FacilityID': df['PUFacilityID'],
                           'PU_Hour': df['PU_Hour'],
                           'PU_Bucket': df['PUBucket'],
                           'DO_FacilityID': df['DOFacilityID'],
                           'DO_Hour': df['DO_Hour'],
                           'DO_Bucket': df['DOBucket'],
                           'Transit': df['PU_Transit_Minute'],
                           'Dwell': df['PU_Dwell_Minute']
                    }
    facility_hour_ml_df = pd.DataFrame(facility_hour_ml_df)
    hour_ml_agg_df = facility_hour_ml_df.groupby(['LoadID', 'PU_FacilityID', 'DO_FacilityID', 'PU_Bucket', 'DO_Bucket'],
                                                 as_index=False) \
        .agg({'histloadID': 'size', 'similarity': 'median', 'PU_Hour': 'mean', 'DO_Hour': 'mean', 'Transit': 'mean', 'Dwell': 'mean'}) \
        .rename(columns={'histloadID': 'count'}) \
        .sort_values(by=['LoadID', 'count', 'similarity'], ascending=False) \
        .reset_index(drop=True)

    df_rank = hour_ml_agg_df.groupby(['LoadID']).cumcount()
    hour_ml_agg_df_select = hour_ml_agg_df[df_rank <= 3]
    return hour_ml_agg_df_select


def similarity_cal_rules(df):
    df['ref'] = CONFIG.SIMILARITY_REF
    histload_feature = {'weight': df.hist_weight / CONFIG.WEIGHTSCALE,
                        'pallet': df.hist_pallet / CONFIG.WEIGHTSCALE,
                        'length': df.hist_miles / CONFIG.MILESCALE
                        }
    histload_df = pd.DataFrame(histload_feature).astype(np.float32)
    newload_feature = {'weight': df.new_weight / CONFIG.WEIGHTSCALE,
                       'length': df.new_miles / CONFIG.MILESCALE}
    newload_df = pd.DataFrame(newload_feature).astype(np.float32)
    sim_row = ((histload_df.values / norm(histload_df.values, axis=1)[:, np.newaxis]) * (
            newload_df.values / norm(newload_df.values, axis=1)[:, np.newaxis])).sum(axis=1)
    facility_hour_ml_df = {'LoadID': df['LoadID'],
                           'histloadID': df['histloadID'],
                           'similarity': sim_row,
                           'PU_FacilityID': df['PU_FacilityID'],
                           'PU_Hour': df['PU_Hour'],
                           'DO_FacilityID': df['DO_FacilityID'],
                           'DO_Hour': df['DO_Hour']
                           }
    facility_hour_ml_df = pd.DataFrame(facility_hour_ml_df)
    facility_hour_ml_df['PU_Hour_ml'] = facility_hour_ml_df['similarity'] * facility_hour_ml_df['PU_Hour']
    facility_hour_ml_df['DO_Hour_ml'] = facility_hour_ml_df['similarity'] * facility_hour_ml_df['DO_Hour']
    return facility_hour_ml_df