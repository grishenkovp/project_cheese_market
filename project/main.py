import pandas as pd
from datetime import datetime
import yaml
import os
from tqdm import tqdm
from glob import glob
from db import PostgresDB as db
from sql import script_create_table_prices, \
                script_create_table_customers,\
                script_create_table_stocks, \
                script_create_table_sales, \
                script_add_table_sales_start_records
from customer import Customer as c
from buy import Buy as b
import model as m

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_data_dataset = settings['DATA']['PATH']

def append_prices() -> None:
    """Заполняем в БД реестр с ценами"""
    # Формируем шаблон таблицы в БД
    db().general_script(script_create_table_prices)
    # Записываем в БД информацию по ценам за декабрь 2019 и весь 2020 год в разрезе торговых сетей
    for _ in glob(os.path.join(path_data_dataset, "*.csv")):
        db().write_table_csv(_, m.DB_TABLE_PRICES)
    print("The script is executed!")

def append_customes() -> None:
    """Заполняем в БД реестр с покупателями"""
    # Формируем шаблоны таблиц в БД
    db().general_script(script_create_table_customers)
    db().general_script(script_create_table_stocks)
    # Рандомным образом генерируем всех покупателей города и их начальные остатки продукции на начало 2020 года
    customer_df, stock_df = c.generation_dataframe_customer()
    # Записываем информацию в БД
    db().write_table_dataframe(customer_df, m.DB_TABLE_CUSTOMERS)
    db().write_table_dataframe(stock_df, m.DB_TABLE_STOCKS_CHEESE)
    print("The script is executed!")


def append_sales_start() -> None:
    """Заполняем в БД реестр с продажами на 31.12.2019"""
    # Формируем шаблон таблицы в БД
    db().general_script(script_create_table_sales)
    # Формируем записи со стартовыми продажами.
    # Данные по объемам купленного сыра за декабрь это остатки на начало 2020 года.
    # Цены подтягиваются из прайс-листа (таблица prices) за декарь 2019 в разрезе ритейлер-вид сыра.
    db().add_table_sales_start_records(script_add_table_sales_start_records, m.DATE_BASE, m.MODEL_WORK)
    print("The script is executed!")


def append_sales() -> None:
    """Формирование покупок"""
    # Количество покупателей
    customer_count = sum(m.CUSTOMER_PROPORTION.values())
    for dt in tqdm(pd.date_range(m.DATE_START,m.DATE_FINISH)):
        dt = datetime.strftime(dt, '%d.%m.%Y')
        for customer_id in range(1, customer_count + 1):
            b().make_decision_purchase_product(dt,customer_id)
            b().update_customer_stocks(customer_id)
            b().process_purchase(dt,customer_id)
    print("The script is executed!")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # append_prices()
    # append_customes()
    # append_sales_start()
    append_sales()







