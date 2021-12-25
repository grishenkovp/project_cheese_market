import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import yaml
from typing import Dict

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

user = settings['DB']['USER']
password = settings['DB']['PASSWORD']
host = settings['DB']['HOST']
post = settings['DB']['PORT']
name = settings['DB']['NAME']


class PostgresDB():
    """Работа с БД"""

    def __init__(self, user: str = user,
                 password: str = password,
                 host: str = host,
                 post: int = post,
                 name: str = name):
        self.user = user
        self.password = password
        self.host = host
        self.post = post
        self.name = name

    def create_point_connect(self):
        point_connect = 'postgresql://{}:{}@{}:{}/{}'.format(self.user,
                                                             self.password,
                                                             self.host,
                                                             self.post,
                                                             self.name)
        return point_connect

    def general_script(self, script_sql: str) -> None:
        """Универсальный тип запроса"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        cursor.execute(script_sql)
        conn.commit()
        cursor.close()
        conn.close()

    def get_customer_data(self, script_sql: str, customer_id: int) -> Dict:
        """Запрос данных по покупателю"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(customer_id=customer_id)
        cursor.execute(script_sql_current)
        customer_data = cursor.fetchone()
        customer_data_dict = {}
        customer_data_dict["customer_id"] = customer_data[0]
        customer_data_dict["family_type"] = customer_data[1]
        customer_data_dict["number_people"] = customer_data[2]
        customer_data_dict["financial_wealth"] = customer_data[3]
        customer_data_dict["favorite_kind_cheese"] = customer_data[4]
        customer_data_dict["avg_cheese_consumption"] = customer_data[5]
        customer_data_dict["stop_eating_cheese"] = customer_data[6]
        customer_data_dict["switching_another_cheese"] = customer_data[7]
        customer_data_dict["basic_retailer"] = customer_data[8]
        customer_data_dict["significant_price_change"] = customer_data[9]
        customer_data_dict["looking_cheese"] = customer_data[10]
        customer_data_dict["sensitivity_marketing_campaign"] = customer_data[11]
        customer_data_dict["key"] = customer_data[12]
        customer_data_dict["current_value_cheese"] = customer_data[13]
        conn.commit()
        cursor.close()
        conn.close()
        return customer_data_dict

    def get_customer_last_price(self, script_sql: str, customer_id: int, model: str) -> int:
        """Запрос цены из последней операции пользователя"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(customer_id=customer_id, model=model)
        cursor.execute(script_sql_current)
        customer_data_last_price = cursor.fetchone()
        customer_last_price = customer_data_last_price[0]
        conn.commit()
        cursor.close()
        conn.close()
        return customer_last_price

    def get_retailer_current_price(self, script_sql: str, dt, model: str, retailer: str, kind_cheese: str) -> int:
        """Получение актуальной цены в разрезе дата-ритейлер-вид сыра"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(dt=dt, model=model, retailer=retailer, kind_cheese=kind_cheese)
        cursor.execute(script_sql_current)
        retailer_current_price = cursor.fetchone()
        current_price = retailer_current_price[0]
        conn.commit()
        cursor.close()
        conn.close()
        return current_price

    def get_retailer_cheaper_kind_cheese(self, script_sql: str,
                                         dt, model: str,
                                         retailer: str,
                                         retailer_current_price: int) -> Dict:
        """Получить по указанном ритейлеру на заданную дату вид сыра, цена на который меньше или равна цене
        на текущий вид сыра"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(dt=dt,
                                               model=model,
                                               retailer=retailer,
                                               retailer_current_price=retailer_current_price)
        cursor.execute(script_sql_current)
        retailer_cheaper_kind_cheese = cursor.fetchone()
        retailer_cheaper_kind_cheese_data_dict = {}
        retailer_cheaper_kind_cheese_data_dict["kind_cheese"] = retailer_cheaper_kind_cheese[0]
        retailer_cheaper_kind_cheese_data_dict["price"] = retailer_cheaper_kind_cheese[1]
        conn.commit()
        cursor.close()
        conn.close()
        return retailer_cheaper_kind_cheese_data_dict

    def get_retailer_min_price(self, script_sql: str,
                               dt, model: str,
                               current_kind_cheese: str,
                               retailer_top_list: str) -> Dict:
        """ Получить ритейлера с самой конкурентной ценой на текущий вид сыра"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(dt=dt,
                                               model=model,
                                               current_kind_cheese=current_kind_cheese,
                                               retailer_top_list=retailer_top_list)
        cursor.execute(script_sql_current)
        retailer_min_price = cursor.fetchone()
        retailer_min_price_data_dict = {}
        retailer_min_price_data_dict["retailer"] = retailer_min_price[0]
        retailer_min_price_data_dict["price"] = retailer_min_price[1]
        conn.commit()
        cursor.close()
        conn.close()
        return retailer_min_price_data_dict

    def update_customer_stocks(self, script_sql: str, customer_id: int, current_value: int) -> None:
        """Обновить данные по товарным запасам покупателя"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(customer_id=customer_id,
                                               current_value=current_value)
        cursor.execute(script_sql_current)
        conn.commit()
        cursor.close()
        conn.close()

    def add_table_sales_start_records(self, script_sql: str, dt, model: str) -> None:
        # Формируем записи со стартовыми продажами.
        # Данные по объемам купленного сыра за декабрь это остатки на начало 2020 года.
        # Цены подтягиваются из прайс-листа (таблица prices) за декарь 2019 в разрезе ритейлер-вид сыра
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(dt=dt, model=model)
        cursor.execute(script_sql_current)
        conn.commit()
        cursor.close()
        conn.close()

    def add_table_sales_record(self, script_sql: str,
                               dt,
                               model: str,
                               retailer: str,
                               customer_id: int,
                               kind_cheese: str,
                               quantity: int,
                               price: int) -> None:
        # Формируем запись в БД о покупке товара
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        script_sql_current = script_sql.format(dt=dt,
                                               model=model,
                                               retailer=retailer,
                                               customer_id=customer_id,
                                               kind_cheese=kind_cheese,
                                               quantity=quantity,
                                               price=price)
        cursor.execute(script_sql_current)
        conn.commit()
        cursor.close()
        conn.close()

    def read_table_db(self, script_sql: str, dt) -> None:
        """Запрос данных по продажам из БД"""
        point = self.create_point_connect()
        conn = psycopg2.connect(point)
        script_sql_current = script_sql.format(dt=dt)
        return pd.read_sql(script_sql_current, conn)

    def write_table_db_csv(self, name_dataset: str, name_db_table: str, dt) -> None:
        """Запись данных в БД из текстового файла формата csv"""
        point = self.create_point_connect()
        engine = create_engine(point)
        df = pd.read_csv(name_dataset, sep=";", parse_dates=["date"], dayfirst=True)
        df = df[df['date'] <= dt]
        df.to_sql(name_db_table, engine, if_exists='append', index=False)

    def write_table_db_dataframe(self, df, name_db_table: str) -> None:
        """Запись данных в БД из датафрейма Pandas"""
        point = self.create_point_connect()
        engine = create_engine(point)
        df.to_sql(name_db_table, engine, if_exists='append', index=False)
