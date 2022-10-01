# Функция получения ретеншен_дф в когортах по дням с пользовательскими временными параметрами
    
def calculate_RR(path_to_reg_data, path_to_auth_data):
    """calculate retention rate by cohort

    Args:
        path_to_reg_data (str): path to dataset with dates of registrations
        path_to_auth_data (str): path to dataset with dates of authorizations

    Returns:
        Data Frame: with retention rate
    """
    
    import pandas as pd
    import numpy as np
    from datetime import datetime, date

    reg_data = pd.read_csv(path_to_reg_data, sep=';')
    auth_data = pd.read_csv(path_to_auth_data, sep=';')
    
    reg_data['reg_ts'] = pd.to_datetime(reg_data['reg_ts'], unit='s').dt.date
    auth_data['auth_ts'] = pd.to_datetime(auth_data['auth_ts'], unit='s').dt.date
    
    date_start = pd.to_datetime(str(input('Введите дату начала периода в формате YYYY-MM-DD: '))).date()
    date_end = pd.to_datetime(str(input('Введите дату окончания периода в формате YYYY-MM-DD: '))).date()
    
    reg_data_subset = reg_data.query("@date_start <= reg_ts <= @date_end")
    
    df_period = auth_data.merge(reg_data_subset, on='uid', how='inner')
    
    df_cohort = df_period.groupby(['reg_ts', 'auth_ts'], as_index=False)\
                    .agg({'uid': 'nunique'})\
                    .rename(columns={'uid': 'num_users'})
    
    df_cohort['days_dif'] = df_cohort['auth_ts'] - df_cohort['reg_ts']
    df_cohort['days_dif'] = df_cohort['days_dif'].dt.days
    
    days_limit = int(input('Введите верхнюю границу количества дней retention: '))
    df_cohort = df_cohort[df_cohort['days_dif'] <= days_limit]
    
    cohort_pivot = df_cohort.pivot_table(index='reg_ts', columns='days_dif', values='num_users')
    
    cohort_size = cohort_pivot[0]
    retention_df = cohort_pivot.divide(cohort_size, axis=0).mul(100).round(2)
    
    return retention_df
