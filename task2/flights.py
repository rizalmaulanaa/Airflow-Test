import os
import pandas as pd

from glob import glob
from datetime import datetime, timedelta


def get_data_daily(path, **kwargs):
    date_exe = kwargs['logical_date']
    path_comb = path + 'Full_Table.csv'
    month = '0'+str(date_exe.month) if len(str(date_exe.month)) == 1 else str(date_exe.month)
    day = '0'+str(date_exe.day) if len(str(date_exe.day)) == 1 else str(date_exe.day)
    path_dataset = glob(path + 'dataset/*{}-{}-{}.csv'.format(date_exe.year, month, day))

    if not os.path.isfile(path_comb):
        file = open(path_comb, 'w')
        file.close()

    if path_dataset != []:
        try:
            df_full = pd.read_csv(path_comb)
            df = pd.concat([df_full] + [pd.read_csv(path_file) for path_file in path_dataset])
        except:
            df = pd.concat([pd.read_csv(path_file) for path_file in path_dataset])

        df.index = range(len(df.index))
        df.to_csv(path_comb, mode='w', index=False)

def get_data_monthly(path, **kwargs):
    date_exe = kwargs['logical_date']
    path_comb = path + 'Full_Table_Monthly.csv'

    if not os.path.isfile(path_comb):
        file = open(path_comb, 'w')
        file.close()

    if date_exe.day == 1:
        date_exe = (date_exe - timedelta(days=1)).date()
        month = '0'+str(date_exe.month) if len(str(date_exe.month)) == 1 else str(date_exe.month)
        path_dataset = glob(path + 'dataset/*{}-{}-*.csv'.format(date_exe.year, month))

        if path_dataset != []:
            try:
                df_full = pd.read_csv(path_comb)
                df = pd.concat([df_full] + [pd.read_csv(path_file) for path_file in path_dataset])
            except:
                df = pd.concat([pd.read_csv(path_file) for path_file in path_dataset])

            df = df[df['rank'] ==  1]
            df.index = range(len(df.index))
            df.to_csv(path_comb, mode='w', index=False)

def get_data_annual(path, **kwargs):
    date_exe = kwargs['logical_date']
    path_comb = path + 'Full_Table_Annual.csv'

    if not os.path.isfile(path_comb):
        file = open(path_comb, 'w')
        file.close()

    if date_exe.month == 1:
        date_exe = (date_exe - timedelta(days=1)).date()
        path_dataset = glob(path + 'dataset/*{}-*.csv'.format(date_exe.year))

        if path_dataset != []:
            try:
                df_full = pd.read_csv(path_comb)
                df = pd.concat([df_full] + [pd.read_csv(path_file) for path_file in path_dataset])
            except:
                df = pd.concat([pd.read_csv(path_file) for path_file in path_dataset])

            df = df[df['rank'] ==  1]
            df.index = range(len(df.index))
            df.to_csv(path_comb, mode='w', index=False)