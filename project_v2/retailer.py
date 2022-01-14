import pandas as pd
import yaml
import os
from datetime import datetime
from typing import List, Dict
import model as m

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_data_prices = settings['DATA']['PATH_PRICES']


class Retailer():
    def __init__(self):
        pass

    @classmethod
    def get_dataframe_sales(cls, model: int) -> pd.DataFrame:
        """Получение текущего прайс-листа с ценами"""
        print('Получение прайс-листа модели')
        full_path_file = os.path.join(path_data_prices, m.FILE_NAME_PATTERN_PRICES.format(model))
        dateparser = lambda x: datetime.strptime(x, "%d.%m.%Y")
        df_sales = pd.read_csv(full_path_file, sep=';', parse_dates=['date'], date_parser=dateparser)
        return df_sales

    @classmethod
    def get_retailer_current_price(cls,
                                   df_sales: pd.DataFrame,
                                   dt: str,
                                   basic_retailer: str,
                                   favorite_kind_cheese: str) -> int:
        """Получение текущей цены на указанный вид сыра в конкретном ритейлере"""
        dt = datetime.strptime(dt, "%d.%m.%Y")
        current_price = df_sales[(df_sales['date'] == dt) &
                                 (df_sales['retailer'] == basic_retailer) &
                                 (df_sales['kind_cheese'] == favorite_kind_cheese)
                                 ]['price'].values[0]
        return current_price

    @classmethod
    def get_retailer_min_price(cls,
                               df_sales: pd.DataFrame,
                               dt: str,
                               favorite_kind_cheese: str,
                               retailer_top: List):
        """Получить ритейлера с самой конкурентной ценой на текущий вид сыра"""
        dt = datetime.strptime(dt, "%d.%m.%Y")
        df = df_sales[(df_sales['date'] == dt) &
                      (df_sales['kind_cheese'] == favorite_kind_cheese) &
                      (df_sales['retailer'].isin(retailer_top))
                      ][['retailer', 'price']]
        df = df.sort_values(by='price').values[0]
        retailer = df[0]
        price = df[1]
        return retailer, price

    @classmethod
    def get_retailer_cheaper_kind_cheese(cls,
                                         df_sales: pd.DataFrame,
                                         dt: str,
                                         retailer: str,
                                         retailer_current_price: int):
        """Получить по ритейлеру на заданную дату вид сыра, цена на который меньше или равна цене на текущий вид сыра"""
        dt = datetime.strptime(dt, "%d.%m.%Y")
        df = df_sales[(df_sales['date'] == dt) &
                      (df_sales['retailer'] == retailer) &
                      (df_sales['price'] <= retailer_current_price)
                      ][['kind_cheese', 'price']]
        df = df.sort_values(by='price', ascending=False).values
        if len(df) == 1:
            kind_cheese = df[0][0]
            price = df[0][1]
        else:
            kind_cheese = df[1][0]
            price = df[1][1]
        return kind_cheese, price
