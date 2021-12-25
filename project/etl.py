import pandas as pd
import datetime
import yaml
import os
from glob import glob

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_data_sales = settings['DATA']['PATH_SALES']
path_data_sales_group = settings['DATA']['PATH_SALES_GROUP']
file_name_sales_group = settings['DATA']['FILE_NAME_SALES_GROUP']


def write_dataframe_csv(df, path_data_dataset: str, name_dataset: str) -> None:
    """Записать датафрейм в файл формата csv"""
    df.to_csv(os.path.join(path_data_dataset, name_dataset), sep=',', index=False)


def group_datasets_sales():
    """Сгруппировать данные с продажами по разным моделям в единый файл"""
    list_df = []

    dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d')

    for _ in glob(os.path.join(path_data_sales, "*.csv")):
        df_current = pd.read_csv(_, sep=',', parse_dates=['date'], date_parser=dateparse)
        list_df.append(df_current)

    df_full = pd.concat(list_df, ignore_index=True)

    # Перевод веса в килограммы
    df_full['quantity'] = df_full['quantity'] / 1000

    df_full['proceeds'] = df_full['quantity'] * df_full['price']

    df_full['week_number'] = df_full['date'].apply(lambda x: x.strftime("%V"))

    df_group = df_full.groupby(by=['model', 'week_number', 'retailer', 'kind_cheese'], as_index=False).agg(
        {'quantity': ['sum'],
         'proceeds': ['sum', 'size']})

    df_group.columns = ['model', 'week_number', 'retailer', 'kind_cheese', 'total_quantity', 'total_proceeds',
                        'total_amount']

    df_group['total_quantity'] = df_group['total_quantity'].apply(lambda x: round(x, 2))
    df_group['total_proceeds'] = df_group['total_proceeds'].apply(lambda x: round(x, 2))

    path_file = '{path_file}{file_name}'.format(path_file=path_data_sales_group, file_name=file_name_sales_group)
    df_group.to_csv(path_file, index=False, sep=';', decimal=',')
    print("The script is executed!")
