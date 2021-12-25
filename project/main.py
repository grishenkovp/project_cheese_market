import pandas as pd
from datetime import datetime
import time
import yaml
import os
from tqdm import tqdm
from glob import glob
from typing import List
from db import PostgresDB as db
from sql import script_create_table_prices, \
    script_create_table_customers, \
    script_create_table_stocks, \
    script_create_table_sales, \
    script_add_table_sales_start_records, \
    script_read_table_sales
from customer import Customer as c
from buy import Buy as b
import model as m
from etl import group_datasets_sales, write_dataframe_csv

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_data_prices = settings['DATA']['PATH_PRICES']
path_sales = settings['DATA']['PATH_SALES']


def append_prices(number_model: int) -> None:
    """Заполняем в БД реестр с ценами"""
    # Формируем шаблон таблицы в БД
    db().general_script(script_create_table_prices)
    # Записываем в БД информацию по ценам за декабрь 2019 и весь 2020 год в разрезе торговых сетей
    for _ in glob(os.path.join(path_data_prices, "*.csv")):
        if _.find(str(number_model)) != -1:
            db().write_table_db_csv(_, m.DB_TABLE_PRICES, m.DATE_FINISH)
    print("The script is executed!")


def append_customes() -> None:
    """Заполняем в БД реестр с покупателями"""
    # Формируем шаблоны таблиц в БД
    db().general_script(script_create_table_customers)
    db().general_script(script_create_table_stocks)
    # Рандомным образом генерируем всех покупателей города и их начальные остатки продукции на начало 2020 года
    customer_df, stock_df = c.generation_dataframe_customer()
    # Записываем информацию в БД
    db().write_table_db_dataframe(customer_df, m.DB_TABLE_CUSTOMERS)
    db().write_table_db_dataframe(stock_df, m.DB_TABLE_STOCKS_CHEESE)
    print("The script is executed!")


def append_sales_start(number_model: int) -> None:
    """Заполняем в БД реестр с продажами на 31.12.2019"""
    # Формируем шаблон таблицы в БД
    db().general_script(script_create_table_sales)
    # Формируем записи со стартовыми продажами.
    # Данные по объемам купленного сыра за декабрь это остатки на начало 2020 года.
    # Цены подтягиваются из прайс-листа (таблица prices) за декарь 2019 в разрезе ритейлер-вид сыра.
    db().add_table_sales_start_records(script_add_table_sales_start_records, m.DATE_BASE, number_model)
    print("The script is executed!")


def append_sales(number_model: int) -> None:
    """Формирование покупок"""
    # Количество покупателей
    customer_count = sum(m.CUSTOMER_PROPORTION.values())
    for dt in tqdm(pd.date_range(m.DATE_START, m.DATE_FINISH)):
        dt = datetime.strftime(dt, '%d.%m.%Y')
        for customer_id in range(1, customer_count + 1):
            b().make_decision_purchase_product(dt=dt, model=number_model, customer_id=customer_id)
            b().update_customer_stocks(customer_id=customer_id)
            b().process_purchase(dt, model=number_model, customer_id=customer_id)
    print("The script is executed!")


def read_table_sales():
    """Читаем из БД таблицу Продажи с текущими данными начиная с 01.01.2020"""
    return db().read_table_db(script_read_table_sales, m.DATE_START)


def write_dataframe_sales_csv(df, number_model: int):
    """Формируем файл csv c продажами в рамках текущей модели"""
    name_dataset_current = 'sales_model_{number_model}.csv'.format(number_model=number_model)
    write_dataframe_csv(df=df,
                        path_data_dataset=path_sales,
                        name_dataset=name_dataset_current)
    print("The script is executed!")


def execute_all_func(list_model: List):
    """Оркестратор всех команд"""
    for _ in list_model:
        print("Model {number_model}. Start".format(number_model=_))
        append_prices(_)
        time.sleep(1)
        append_customes()
        time.sleep(1)
        append_sales_start(_)
        time.sleep(1)
        append_sales(_)
        time.sleep(1)
        write_dataframe_sales_csv(read_table_sales(), _)
        print("Model {number_model}. Finish".format(number_model=_))
    group_datasets_sales()
    print("All script are executed!")


if __name__ == '__main__':
    execute_all_func(m.LIST_MODEL_WORK)
