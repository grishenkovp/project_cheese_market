import random
import time
from db import PostgresDB as db
from customer import Customer as c
from retailer import Retailer as r
import model as m
from sql import script_add_table_sales_record

class Buy():
    """Формирование списка покупок"""
    # Словарь с данными текущего покупателя и текущим остатком сырного запаса
    customer_data_dict = {}
    # Последняя цена, по которой текущий покупатель приобретал свой любимый сыр
    customer_last_price = None
    # Текущая цена на основной вид сыра пользователя в его основной торговой сети
    retailer_current_price = None
    # Минимальная цена на основной вид сыра текущего пользователя среди 5 основных ритейлеров города
    retailer_current_min_price_val = None
    # Название ритейлера с мин. ценой на основной вид сыра текущего пользователя среди 5 основных ритейлеров города
    retailer_current_min_price_retailer = None
    # Баланс запаса сыра покупателя с учетом ежедневного потребления
    balance_cheese = None
    # Словарь с данными для формирования записей таблицы с продажами в БД
    customer_buy_dict = {}
    # Номер решения
    number_option = None

    def __init__(self):
        pass

    @classmethod
    def get_buy_customer_data(cls,customer_id:int) -> None:
        """Получить данные по ИД покупателя"""
        dict_current = c().get_customer_data(customer_id=customer_id)
        for _ in dict_current:
            cls.customer_data_dict[_] = dict_current[_]

    @classmethod
    def get_buy_customer_last_price(cls,customer_id:int) -> None:
        """Получить последнюю цену сыра, по которой покупатель приобретал товар"""
        last_price_current = c().get_customer_last_price(customer_id = customer_id,
                                                          model = m.MODEL_WORK)
        cls.customer_last_price = last_price_current

    @classmethod
    def get_buy_retailer_current_price(cls, dt):
        """Получить актуальную цеу по дате, ритейлеру, виду сыра"""

        current_price_val = r().get_retailer_current_price(dt=dt,
                                              model = m.MODEL_WORK,
                                              basic_retailer = cls.customer_data_dict.get('basic_retailer'),
                                              favorite_kind_cheese = cls.customer_data_dict.get('favorite_kind_cheese'))
        cls.retailer_current_price = current_price_val

    @classmethod
    def get_retailer_cheaper_kind_cheese(cls,dt)->None:
        """Получить по ритейлеру на заданную дату вид сыра, цена на который меньше или равна цене
        на текущий вид сыра"""
        return r().get_retailer_cheaper_kind_cheese(dt=dt,
                                                    model=m.MODEL_WORK,
                                                    retailer=cls.customer_data_dict.get("basic_retailer"),
                                                    retailer_current_price=cls.retailer_current_price)

    @classmethod
    def get_retailer_min_price(cls, dt) -> None:
        """Получить ритейлера с самой конкурентной ценой на текущий вид сыра"""
        return  r().get_retailer_min_price(dt=dt,
                                     model=m.MODEL_WORK,
                                     current_kind_cheese=cls.customer_data_dict.get('favorite_kind_cheese'),
                                     retailer_top_list=m.RETAILER_TOP_LIST)

    @classmethod
    def make_decision_purchase_product (cls,dt,customer_id) -> None:
        """Реализация процесса принятия решения о покупке товара"""
        cls.customer_data_dict.clear()
        cls.customer_last_price = None
        cls.retailer_current_price = None
        cls.retailer_current_min_price_price = None
        cls.retailer_current_min_price_retailer = None
        cls.customer_buy_dict.clear()
        cls.number_option = None

        cls.get_buy_customer_data(customer_id=customer_id)
        cls.get_buy_customer_last_price(customer_id=customer_id)
        cls.get_buy_retailer_current_price(dt=dt)

        # 1
        # Определим текущий баланс продукта у покупателя
        cls.balance_cheese = None
        delta = cls.customer_data_dict.get('current_value_cheese') - cls.customer_data_dict.get('avg_cheese_consumption')
        cls.balance_cheese = delta if delta > 0 else 0

        # Цикл for применен для имитации синтаксической конструкции if...elif
        # Появляется возможность встраивать дополнительный код между секциями
        # Происходит мгновенный выход из цикла при первом найдем значении без перебора остальных условий
        for _ in range(1):
            # Рассмотрим ветвь решений, когда баланс больше нуля
            # 2
            # Если ритейлер снизил цены и есть чувствительность к маркетинговым взаимодействиям, 
            # Запас продукта меньше 1000 грамм, покупаем до 500 грамм
            if (cls.balance_cheese > 0) \
                and (cls.balance_cheese < 1000) \
                and (cls.retailer_current_price < cls.customer_last_price) \
                and (cls.customer_data_dict.get('sensitivity_marketing_campaign')=='да'):
                cls.number_option = 2
                break

            # 3
            # Если ритейлер снизил цены, но нет чувствительности к маркетинговым взаимодействиям
            # или запас товара больше 1000 грамм
            # Покупку не совершаем
            if (cls.balance_cheese > 0) \
                and (cls.retailer_current_price < cls.customer_last_price) \
                and ((cls.balance_cheese >= 1000) or
                     (cls.customer_data_dict.get('sensitivity_marketing_campaign')=='нет')):
                cls.number_option = 3
                break

            # Рассмотрим ветвь решений, когда баланс равен 0
            # 4
            # Если запасы сыра равны нулю и уровень благосостояния покупателя высокий,то он приобретает товар по любой цене
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth')=='высокий'):
                cls.number_option = 4
                break

            # 5
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # то он сходу будет приобретать товар, только если новая текущая цена на сыр меньше или равна предыдущей
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price <= cls.customer_last_price):
                cls.number_option = 5
                break

            # 6
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены не превышает индивидуальный порог чувствительности
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)<= \
                     cls.customer_data_dict.get("significant_price_change")):
                cls.number_option = 6
                break

            # 7
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # при этом покупатель не готов искать аналогичный товар у других производителей,
            # готов переходить на более дешевый продукт
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese") == "нет") \
                and (cls.customer_data_dict.get("switching_another_cheese") == "да"):
                cls.number_option = 7
                break

            # 8
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # при этом покупатель не готов искать аналогичный товар у других производителей,
            # не готов переходить на более дешевый продукт,
            # готов на время полность отказаться от потребления
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="нет") \
                and (cls.customer_data_dict.get("switching_another_cheese")=="нет") \
                and (cls.customer_data_dict.get("stop_eating_cheese")=="да"):
                cls.number_option = 8
                break

            # 9
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # при этом покупатель не готов искать аналогичный товар у других производителей,
            # не готов переходить на более дешевый продукт,
            # не готов на время полность отказаться от потребления
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="нет") \
                and (cls.customer_data_dict.get("switching_another_cheese")=="нет") \
                and (cls.customer_data_dict.get("stop_eating_cheese")=="нет"):
                cls.number_option = 9
                break

            cls.retailer_current_min_price_retailer, cls.retailer_current_min_price_val = \
                cls.get_retailer_min_price(dt=dt)

            # 10
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # цена на найденный товар аналогична или меньше предыдущей цены
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="да") \
                and (cls.retailer_current_min_price_val <= cls.customer_last_price):
                cls.number_option = 10
                break

            # 11
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чувствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены не превышает индивидуальный порог чувствительности
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="да") \
                and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                and ((((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) \
                     <= cls.customer_data_dict.get("significant_price_change")):
                cls.number_option = 11
                break

            # 12
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чувствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # готов переходить на более дешевый продукт
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="да") \
                and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                and ((((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("switching_another_cheese") == "да"):
                cls.number_option = 12
                break

            # 13
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чувствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # не готов переходить на более дешевый продукт
            # готов отказаться от покупки продукта
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price)/cls.customer_last_price)*100)> \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese")=="да") \
                and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                and ((((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                and (cls.customer_data_dict.get("stop_eating_cheese")=="да"):
                cls.number_option = 13
                break

            # 14
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чувствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чувствительности
            # не готов переходить на более дешевый продукт
            # не готов отказаться от покупки продукта
            if (cls.balance_cheese == 0) \
                and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                and (cls.retailer_current_price > cls.customer_last_price) \
                and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                     cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("looking_cheese") == "да") \
                and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                and ((((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                cls.customer_data_dict.get("significant_price_change")) \
                and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                and (cls.customer_data_dict.get("stop_eating_cheese") == "нет"):
                cls.number_option = 14
                break

        # Номера соответствуют прилагаемой схеме принятия решения
        if cls.number_option in [2]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_LIST_VALUE_CHEESE if _ <= 500])
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [3, 8, 13]:
            pass

        elif cls.number_option in [4, 5, 6]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice(m.BUY_LIST_VALUE_CHEESE)
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [7, 12]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'], \
            cls.customer_buy_dict['price'] = cls.get_retailer_cheaper_kind_cheese(dt=dt)
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_LIST_VALUE_CHEESE if _ <= 500])
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [9, 14]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_LIST_VALUE_CHEESE if _ <= 250])
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [10, 11]:
            cls.customer_buy_dict['retailer'] = cls.retailer_current_min_price_retailer
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice(m.BUY_LIST_VALUE_CHEESE)
            cls.customer_buy_dict['price'] = cls.retailer_current_min_price_val
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

    @classmethod
    def update_customer_stocks(cls,customer_id:int) -> None:
        """Обновляем данные о товарном запасе покупателя"""
        c().update_customer_stocks(customer_id=customer_id, balance_cheese=cls.balance_cheese)
        cls.balance_cheese = None

    @classmethod
    def process_purchase(cls, dt, customer_id: int, script=script_add_table_sales_record) -> None:
        """Записать данные о покупке в БД"""
        if cls.customer_buy_dict!={}:
            db().add_table_sales_record(script,
                                        dt=dt,
                                        model=m.MODEL_WORK,
                                        retailer = cls.customer_buy_dict.get('retailer'),
                                        customer_id = customer_id,
                                        kind_cheese = cls.customer_buy_dict.get('kind_cheese'),
                                        quantity = cls.customer_buy_dict.get('quantity'),
                                        price = cls.customer_buy_dict.get('price'))
            cls.customer_buy_dict.clear()
