import pandas as pd
from datetime import datetime
from tqdm import tqdm
from typing import List, Dict
from customer import Customer as c
from retailer import Retailer as r
from buy import Buy as b
import model as m
import etl


def execute_all_func(list_model: List):
    for model_number in list_model:
        print("Модель {}".format(model_number))
        # Генерируем генерируем информацию о покупателях
        dict_customer = c.generation_dict_customer()
        # Записываем данные о покупателях в отдельный файл
        c.write_customers_info_csv(dict_customer, model_number)
        # Генерируем их товарные остатки
        dict_stock = c.generation_dict_stock(dict_customer)
        # Получаем текущий прайс-лист
        df_sales = r.get_dataframe_sales(model_number)
        # Заполняем словарь с ценами последней покупки
        dict_last_price = c.generation_dict_last_price(dict_customer, m.DATE_BASE, df_sales)

        customer_count = sum(m.CUSTOMER_PROPORTION.values())
        print('Формирование книги покупок по модели номер {}'.format(model_number))
        for dt in tqdm(pd.date_range(datetime.strptime(m.DATE_START, "%d.%m.%Y"),
                                     datetime.strptime(m.DATE_FINISH, "%d.%m.%Y"))):
            dt = datetime.strftime(dt, '%d.%m.%Y')
            for customer_id in range(1, customer_count + 1):
                # Принимаем решение о покупке
                b.make_decision_purchase_product(df_sales,
                                                 dict_customer,
                                                 dict_stock,
                                                 dict_last_price,
                                                 dt,
                                                 customer_id)
                # Обновляем данные о товарном остатке покупателя
                b.update_customer_stocks(dict_stock, customer_id)
                # Если совершена покупка, добавляем данные о ней в список
                b.process_purchase(dt, model_number, customer_id)
                # Если совершена покупка, обновляем данные о последней цене,по которой приобретен товар
                b.update_customer_last_price(dict_last_price, customer_id)
        # Записываем информацию о покупках в файл csv
        b.write_sales_csv(model_number)

        dict_customer.clear()
        dict_stock.clear()
        del df_sales
        dict_last_price.clear()


if __name__ == '__main__':
    execute_all_func(m.LIST_MODEL_WORK)
    etl.group_datasets_sales()
