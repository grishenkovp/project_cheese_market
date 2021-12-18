import pandas as pd
import numpy as np
import copy
from tqdm import tqdm
from typing import List, Dict
import model as m
from db import PostgresDB as db
from sql import script_get_customer_data, \
                script_get_customer_last_price, \
                script_update_customer_stocks

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

    #-------------------------------------------------------------------------------------------------------------------
    # Структура ритейлеров города
    retailer_proportion = m.RETAILER_PROPORTION

    def __init__(self):
        pass

    @classmethod
    def generation_list_customer_proportion(cls,list_with_proportion:List = customer_proportion) -> List[str]:
        """Полный список выбора категорий покупателей"""
        list_customer =[]
        for key, value in list_with_proportion.items():
            for _ in range(value):
                list_customer.append(key)
        return list_customer

    @classmethod
    def choice_val(cls,d):
        """Выбор случайного значения из переданного массива данных"""
        if isinstance(d, list):
            return np.random.choice(d)
        elif isinstance(d, dict):
            return np.random.choice(list(d.keys()),
                                    1,
                                    p=[round(i/sum(d.values()),2) for i in d.values()])[0]
        else:
            pass

    @classmethod
    def generation_dataframe_customer(cls) -> None:
        """Генерация рандомного датафрейма с покупателями"""

        # Раздел Покупатель
        list_customer_id = []
        list_family_type = []
        list_number_people = []
        list_financial_wealth = []

        # Раздел Сыр
        list_favorite_kind_cheese = []
        list_current_stock_cheese = []
        list_avg_cheese_consumption = []
        list_stop_eating_cheese = []
        list_switching_another_cheese = []

        # Раздел Ритейлер
        list_basic_retailer = []
        list_significant_price_change = []
        list_looking_cheese = []
        list_sensitivity_marketing_campaign = []

        # Раздел Ключи
        list_key_kind_cheese_retailer = []

        # Стартовый ИД покупателя
        customer_id_val = 1
        for customer_family_type_val in tqdm(cls.generation_list_customer_proportion()):
            # Раздел Покупатель
            list_customer_id.append(customer_id_val)
            list_family_type.append(customer_family_type_val)
            list_number_people.append(cls.customer_number_people.get(customer_family_type_val))
            list_financial_wealth.append(cls.choice_val(cls.customer_financial_wealth))

            # Раздел Сыр
            # Если благосостояние покупателя высокое, то исключаем продукты сырные из выбора
            if  list_financial_wealth[-1] == "высокий":
                customer_kind_cheese_copy = copy.deepcopy(cls.customer_kind_cheese)
                del customer_kind_cheese_copy["продукты сырные"]
                list_favorite_kind_cheese.append(cls.choice_val(customer_kind_cheese_copy))
                customer_kind_cheese_copy.clear()
            else:
                list_favorite_kind_cheese.append(cls.choice_val(cls.customer_kind_cheese))
            list_current_stock_cheese.append(cls.choice_val(cls.customer_current_stock_cheese))
            list_avg_cheese_consumption.append(cls.customer_number_people.get(customer_family_type_val)*
                                               cls.customer_avg_cheese_consumption)
            list_stop_eating_cheese.append(cls.choice_val(cls.customer_stop_eating_cheese))
            list_switching_another_cheese.append(cls.choice_val(cls.customer_switching_another_cheese))

            # Раздел Ритейлер
            list_basic_retailer.append(cls.choice_val(cls.retailer_proportion))
            list_significant_price_change.append(cls.choice_val(cls.customer_significant_price_change))
            list_looking_cheese.append(cls.choice_val(cls.customer_looking_cheese))
            list_sensitivity_marketing_campaign.append(cls.choice_val(cls.customer_sensitivity_marketing_campaign))

            # Раздел Ключи
            list_key_kind_cheese_retailer.append("|".join([list_basic_retailer[-1],list_favorite_kind_cheese[-1]]))

            customer_id_val = customer_id_val + 1

        customer_data_dict = {"customer_id":list_customer_id,
                        "family_type":list_family_type,
                        "number_people":list_number_people,
                        "financial_wealth":list_financial_wealth,

                        "favorite_kind_cheese":list_favorite_kind_cheese,
                        "avg_cheese_consumption":list_avg_cheese_consumption,
                        "stop_eating_cheese":list_stop_eating_cheese,
                        "switching_another_cheese":list_switching_another_cheese,

                        "basic_retailer":list_basic_retailer,
                        "significant_price_change":list_significant_price_change,
                        "looking_cheese":list_looking_cheese,
                        "sensitivity_marketing_campaign":list_sensitivity_marketing_campaign,
                         "key":list_key_kind_cheese_retailer}

        stock_cheese_data_dict = {"customer_id":list_customer_id,
                                  "current_value":list_current_stock_cheese}

        customer_df = pd.DataFrame(data=customer_data_dict)
        stock_df = pd.DataFrame(data=stock_cheese_data_dict)

        return customer_df, stock_df

    @classmethod
    def get_customer_data(cls, customer_id: int, script=script_get_customer_data) -> Dict:
        """Получить данные по ИД покупателя"""
        customer_data_dict = db().get_customer_data(script, customer_id)
        return customer_data_dict

    @classmethod
    def get_customer_last_price(cls,customer_id:int,model:int, script=script_get_customer_last_price) -> int:
        """Получить последнюю цену сыра, по которой покупатель приобретал товар"""
        last_price_current = db().get_customer_last_price(script,customer_id=customer_id, model = model)
        return last_price_current

    @classmethod
    def update_customer_stocks(cls, customer_id: int, balance_cheese: int, script=script_update_customer_stocks) -> None:
        """Обновить данные по товарным запасам покупателя"""
        db().update_customer_stocks(script, customer_id = customer_id, current_value=balance_cheese)