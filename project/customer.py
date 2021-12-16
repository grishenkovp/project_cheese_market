import pandas as pd
import random
from tqdm import tqdm
from typing import List, Dict
import model as m
from retailer import Retailer as rr
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
    customer_list_significant_price_change = m.CUSTOMER_LIST_SIGNIFICANT_PRICE_CHANGE

    def __init__(self):
        pass

    @classmethod
    def generation_list(cls, list_with_proportion:List) -> List[str]:
        """Генерация списка выбора из частотного словаря"""
        list_generation =[]
        for key, value in list_with_proportion.items():
            for _ in range(value):
                list_generation.append(key)
        return list_generation

    @classmethod
    def generation_list_customer_proportion(cls) -> List[str]:
        """Полный список выбора категорий покупателей"""
        return cls.generation_list(cls.customer_proportion)

    @classmethod
    def generation_list_financial_wealth(cls) -> List[str]:
        """Полный список выбора категорий клиентов по уровню благосостояния"""
        return cls.generation_list(cls.customer_financial_wealth)

    @classmethod
    def generation_list_kind_cheese(cls) -> List[str]:
        """Полный список выбора категорий сыров"""
        return cls.generation_list(cls.customer_kind_cheese)

    @classmethod
    def generation_list_sensitivity_marketing_campaign(cls) -> List[str]:
        """Полный список чувствительности покупателей к маркетинговым акциям"""
        return cls.generation_list(cls.customer_sensitivity_marketing_campaign)

    @classmethod
    def generation_list_stop_eating_cheese(cls) -> List[str]:
        """Полный список готовности покупателей отказаться на время потребления сыра"""
        return cls.generation_list(cls.customer_stop_eating_cheese)

    @classmethod
    def generation_list_switching_another_cheese(cls) -> List[str]:
        """Полный список готовности покупателей переключиться на более дешевый вид сыра"""
        return cls.generation_list(cls.customer_switching_another_cheese)

    @classmethod
    def generation_list_looking_cheese(cls) -> List[str]:
        """Полный список готовности покупателей искать сыр по страрой цене в других продуктовых сетях"""
        return cls.generation_list(cls.customer_looking_cheese)


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
        #list_last_price_buy = []
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
            financial_wealth_current_customer = random.choice(cls.generation_list_financial_wealth())
            list_financial_wealth.append(financial_wealth_current_customer)

            # Раздел Сыр
            # Если благосостояние покупателя высокое, то исключаем продукты сырные из выбора
            if  financial_wealth_current_customer == "высокий":
                customer_kind_cheese_val = random.choice([kind_cheese_val
                                                          for kind_cheese_val in cls.generation_list_kind_cheese()
                                                        if kind_cheese_val!="продукты сырные"])
            else:
                customer_kind_cheese_val = random.choice(cls.generation_list_kind_cheese())

            list_favorite_kind_cheese.append(customer_kind_cheese_val)
            list_current_stock_cheese.append(random.choice(cls.customer_current_stock_cheese))
            list_avg_cheese_consumption.append(cls.customer_number_people.get(customer_family_type_val)*
                                               cls.customer_avg_cheese_consumption)
            list_stop_eating_cheese.append(random.choice(cls.generation_list_stop_eating_cheese()))
            list_switching_another_cheese.append(random.choice(cls.generation_list_switching_another_cheese()))

            # Раздел Ритейлер
            customer_retailer_val = random.choice(rr.generation_list_retailer_proportion())
            list_basic_retailer.append(customer_retailer_val)
            list_significant_price_change.append(random.choice(cls.customer_list_significant_price_change))
            list_looking_cheese.append(
                random.choice(cls.generation_list_looking_cheese()))
            list_sensitivity_marketing_campaign.append(
                random.choice(cls.generation_list_sensitivity_marketing_campaign()))

            # Раздел Ключи
            key_list = [customer_retailer_val,customer_kind_cheese_val]
            list_key_kind_cheese_retailer.append("|".join(key_list))

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