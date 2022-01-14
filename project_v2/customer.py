import pandas as pd
import numpy as np
import copy
import yaml
import os
from datetime import datetime
from tqdm import tqdm
from typing import List, Dict
import model as m

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_customers_info = settings['DATA']['PATH_CUSTOMERS_INFO']


class Customer():
    # Структура покупателей
    customer_proportion = m.CUSTOMER_PROPORTION

    # Число человек в семье
    customer_number_people = m.CUSTOMER_NUMBER_PEOPLE

    # Структура уровня финансового благосостояния покупателей
    customer_financial_wealth = m.CUSTOMER_FINANCIAL_WEALTH

    # Список выбора текущего запаса сыра у покупателя
    customer_current_stock_cheese = m.CUSTOMER_CURRENT_STOCK_CHEESE

    # Среднее потребление сыра на одного человека в день в  граммах
    customer_avg_cheese_consumption = m.CUSTOMER_AVG_CHEESE_CONSUMPTION

    # Структура потребление различных видов сыров
    customer_kind_cheese = m.CUSTOMER_KIND_CHEESE

    # Отношение к маркетинговым акциям
    customer_sensitivity_marketing_campaign = m.CUSTOMER_SENSITIVITY_MARKETING_CAMPAIGN

    # Готовность отказаться на время от потребления сыра
    customer_stop_eating_cheese = m.CUSTOMER_STOP_EATING_CHEESE

    # Готовность переключиться на более дешевый вид сыра
    customer_switching_another_cheese = m.CUSTOMER_SWITCHING_ANOTHER_CHEESE

    # Готовность искать сыр по старой цене в других продуктовых сетях
    customer_looking_cheese = m.CUSTOMER_LOOKING_CHEESE

    # Чувствительный порог изменения старой цены на сыр
    customer_significant_price_change = m.CUSTOMER_SIGNIFICANT_PRICE_CHANGE

    # -------------------------------------------------------------------------------------------------------------------
    # Структура ритейлеров города
    retailer_proportion = m.RETAILER_PROPORTION

    def __init__(self):
        pass

    @classmethod
    def generation_list_customer_proportion(cls, list_with_proportion: List = customer_proportion) -> List[str]:
        """Полный список выбора категорий покупателей"""
        list_customer = []
        for key, value in list_with_proportion.items():
            for _ in range(value):
                list_customer.append(key)
        return list_customer

    @classmethod
    def choice_val(cls, d):
        """Выбор случайного значения из переданного массива данных"""
        if isinstance(d, list):
            return np.random.choice(d)
        elif isinstance(d, dict):
            return np.random.choice(list(d.keys()),
                                    1,
                                    p=[round(i / sum(d.values()), 2) for i in d.values()])[0]
        else:
            pass

    @classmethod
    def generation_dict_customer(cls) -> Dict:
        """Генерация рандомного словаря с покупателями"""
        print('Формирование словаря с данными о покупателях')
        dict_customer = {}
        dict_customer_info = {}
        # Стартовый ИД покупателя
        customer_id_val = 1
        for customer_family_type_val in cls.generation_list_customer_proportion():
            # Раздел Покупатель
            dict_customer_info['family_type'] = customer_family_type_val
            dict_customer_info['number_people'] = cls.customer_number_people.get(customer_family_type_val)
            dict_customer_info['financial_wealth'] = cls.choice_val(cls.customer_financial_wealth)

            # Раздел Сыр
            # Если благосостояние покупателя высокое, то исключаем продукты сырные из выбора
            if dict_customer_info.get('financial_wealth') == "высокий":
                customer_kind_cheese_copy = copy.deepcopy(cls.customer_kind_cheese)
                del customer_kind_cheese_copy["продукты сырные"]
                dict_customer_info['favorite_kind_cheese'] = cls.choice_val(customer_kind_cheese_copy)
                customer_kind_cheese_copy.clear()
            else:
                dict_customer_info['favorite_kind_cheese'] = cls.choice_val(cls.customer_kind_cheese)
            dict_customer_info['avg_cheese_consumption'] = cls.customer_number_people.get(customer_family_type_val) * \
                                                           cls.customer_avg_cheese_consumption
            dict_customer_info['stop_eating_cheese'] = cls.choice_val(cls.customer_stop_eating_cheese)
            dict_customer_info['switching_another_cheese'] = cls.choice_val(cls.customer_switching_another_cheese)

            # Раздел Ритейлер
            dict_customer_info['basic_retailer'] = cls.choice_val(cls.retailer_proportion)
            dict_customer_info['significant_price_change'] = cls.choice_val(cls.customer_significant_price_change)
            dict_customer_info['looking_cheese'] = cls.choice_val(cls.customer_looking_cheese)
            dict_customer_info['sensitivity_marketing_campaign'] = cls.choice_val(
                cls.customer_sensitivity_marketing_campaign)

            dict_customer[customer_id_val] = copy.deepcopy(dict_customer_info)
            dict_customer_info.clear()
            customer_id_val = customer_id_val + 1
        return dict_customer

    @classmethod
    def write_customers_info_csv(cls, df_info: pd.DataFrame, model: int) -> None:
        """Записать данные о покупателях в файл формата csv в рамках текущей модели"""

        list_cusromers_info = []
        for k in df_info.keys():
            list_customers_info_current = []
            df_info_current = df_info.get(k)
            list_customers_info_current.append(k)
            for val in df_info_current.values():
                list_customers_info_current.append(val)
            list_cusromers_info.append(list_customers_info_current)

        list_col = ['customer_id',
                    'family_type',
                    'number_people',
                    'financial_wealth',
                    'favorite_kind_cheese',
                    'avg_cheese_consumption',
                    'stop_eating_cheese',
                    'switching_another_cheese',
                    'basic_retailer',
                    'significant_price_change',
                    'looking_cheese',
                    'sensitivity_marketing_campaign']
        df = pd.DataFrame(list_cusromers_info, columns=list_col)
        list_cusromers_info.clear()

        if not os.path.exists(path_customers_info):
            os.mkdir(path_customers_info)
        full_path_file = os.path.join(path_customers_info, m.FILE_NAME_PATTERN_CUSTOMERS_INFO.format(model))
        df.to_csv(full_path_file, sep=',', index=False)
        print('Запись файла с информацией о покупателях завершена')

    @classmethod
    def generation_dict_stock(cls, dict_customer: Dict) -> Dict:
        print('Формирование словаря с запасами сыра')
        """Генерация рандомного словаря с запасами сыра"""
        dict_stock = {}
        for customer_id in dict_customer.keys():
            dict_stock[customer_id] = cls.choice_val(cls.customer_current_stock_cheese)
        return dict_stock

    @classmethod
    def generation_dict_last_price(cls, dict_customer: Dict, date_base: str, df_sales: pd.DataFrame) -> Dict:
        """Генерация словаря с последней ценой покупки товара"""
        print('Формирование словаря с ценами последней покупки')
        dict_last_price = {}
        dt = datetime.strptime(date_base, "%d.%m.%Y")
        for customer_id in dict_customer.keys():
            customer_current = dict_customer.get(customer_id)
            last_price = df_sales[(df_sales['date'] == dt) &
                                  (df_sales['retailer'] == customer_current.get('basic_retailer')) &
                                  (df_sales['kind_cheese'] == customer_current.get('favorite_kind_cheese'))
                                  ]['price'].values[0]
            dict_last_price[customer_id] = last_price
        return dict_last_price
