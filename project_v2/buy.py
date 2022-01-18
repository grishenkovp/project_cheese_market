import pandas as pd
import random
import copy
import yaml
import os
from typing import List, Dict
from retailer import Retailer as r
import model as m

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

path_sales = settings['DATA']['PATH_SALES']


class Buy():
    """Формирование списка покупок"""
    # Словарь с данными текущего покупателя
    customer_data_dict = {}
    # Остаток продукта у покупателя
    customer_stock_product = None
    # Последняя цена, по которой текущий покупатель приобретал свой любимый сыр
    customer_last_price = None
    # Текущая цена на основной вид сыра пользователя в его основной торговой сети
    retailer_current_price = None
    # Минимальная цена на основной вид сыра текущего пользователя среди 5 основных ритейлеров города
    retailer_current_min_price_val = None
    # Название ритейлера с мин. ценой на основной вид сыра текущего пользователя среди 5 основных ритейлеров города
    retailer_current_min_price_retailer = None
    # Наименование виды сыра, цена на который меньше или равна цене на текущий вид сыра
    retailer_current_cheaper_kind_cheese_val = None
    # Цена вида сыра, цена на который меньше или равна цене на текущий вид сыра
    retailer_current_cheaper_kind_cheese_price = None
    # Баланс запаса сыра покупателя с учетом ежедневного потребления
    balance_cheese = None
    # Словарь с данными для формирования записей таблицы с продажами
    customer_buy_dict = {}
    # Список с продажами
    list_buys = []
    # Номер решения
    number_option = None

    def __init__(self):
        pass

    @classmethod
    def make_decision_purchase_product(cls,
                                       df_sales: pd.DataFrame,
                                       dict_customer: Dict,
                                       dict_stock: Dict,
                                       dict_last_price: Dict,
                                       dt,
                                       customer_id: int) -> None:
        """Реализация процесса принятия решения о покупке товара"""
        cls.customer_data_dict.clear()
        cls.customer_stock_product = None
        cls.customer_last_price = None
        cls.retailer_current_price = None
        cls.retailer_current_min_price_val = None
        cls.retailer_current_min_price_retailer = None
        cls.retailer_current_cheaper_kind_cheese_val = None
        cls.retailer_current_cheaper_kind_cheese_price = None
        cls.customer_buy_dict.clear()
        cls.number_option = None

        # Получить данные по ИД покупателя
        cls.customer_data_dict = copy.deepcopy(dict_customer.get(customer_id))
        # Получить данные по остатку продукта у покупателя
        cls.customer_stock_product = dict_stock.get(customer_id)
        # Получить последнюю цену сыра, по которой покупатель приобретал товар
        cls.customer_last_price = dict_last_price.get(customer_id)
        # Получить актуальную цену по дате, ритейлеру, виду сыра
        cls.retailer_current_price = r.get_retailer_current_price(df_sales,
                                                                  dt,
                                                                  cls.customer_data_dict.get('basic_retailer'),
                                                                  cls.customer_data_dict.get('favorite_kind_cheese'))
        # Получить ритейлера с самой конкурентной ценой на текущий вид сыра
        cls.retailer_current_min_price_retailer, cls.retailer_current_min_price_val = \
            r.get_retailer_min_price(df_sales,
                                     dt,
                                     cls.customer_data_dict.get('favorite_kind_cheese'),
                                     m.RETAILER_TOP)

        # Получить по ритейлеру на заданную дату вид сыра, цена на который меньше или равна цене на текущий вид сыра
        cls.retailer_current_cheaper_kind_cheese_val, cls.retailer_current_cheaper_kind_cheese_price = \
            r.get_retailer_cheaper_kind_cheese(df_sales,
                                               dt,
                                               cls.customer_data_dict.get("basic_retailer"),
                                               cls.retailer_current_price)

        # 1
        # Определим текущий баланс продукта у покупателя
        cls.balance_cheese = None
        delta = cls.customer_stock_product - cls.customer_data_dict.get(
            'avg_cheese_consumption')
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
                    and (cls.customer_data_dict.get('sensitivity_marketing_campaign') == 'да'):
                cls.number_option = 2
                break

            # 3
            # Если ритейлер снизил цены, но нет чувствительности к маркетинговым взаимодействиям
            # или запас товара больше 1000 грамм
            # Покупку не совершаем
            if (cls.balance_cheese > 0) \
                    and (cls.retailer_current_price < cls.customer_last_price) \
                    and ((cls.balance_cheese >= 1000) or
                         (cls.customer_data_dict.get('sensitivity_marketing_campaign') == 'нет')):
                cls.number_option = 3
                break

            # Рассмотрим ветвь решений, когда баланс равен 0
            # 4
            # Если запасы сыра равны нулю и уровень благосостояния покупателя высокий,то он приобретает товар по любой цене
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') == 'высокий'):
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
            # текущая цена больше предыдущей, но рост цены не превышает индивидуальный порог чуствительности
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) <= \
                         cls.customer_data_dict.get("significant_price_change")):
                cls.number_option = 6
                break

            # 7
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
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
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # при этом покупатель не готов искать аналогичный товар у других производителей,
            # не готов переходить на более дешевый продукт,
            # готов на время полность отказаться от потребления
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "нет") \
                    and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                    and (cls.customer_data_dict.get("stop_eating_cheese") == "да"):
                cls.number_option = 8
                break

            # 9
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # при этом покупатель не готов искать аналогичный товар у других производителей,
            # не готов переходить на более дешевый продукт,
            # не готов на время полность отказаться от потребления
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "нет") \
                    and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                    and (cls.customer_data_dict.get("stop_eating_cheese") == "нет"):
                cls.number_option = 9
                break

            # 10
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # цена на найденный товар аналогична или меньше предыдущей цены
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "да") \
                    and (cls.retailer_current_min_price_val <= cls.customer_last_price):
                cls.number_option = 10
                break

            # 11
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чуствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены не превышает индивидуальный порог чуствительности
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "да") \
                    and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                    and (
                    (((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) \
                    <= cls.customer_data_dict.get("significant_price_change")):
                cls.number_option = 11
                break

            # 12
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чуствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # готов переходить на более дешевый продукт
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "да") \
                    and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                    and (
                    (((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                    cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("switching_another_cheese") == "да"):
                cls.number_option = 12
                break

            # 13
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чуствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # не готов переходить на более дешевый продукт
            # готов отказаться от покупки продукта
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "да") \
                    and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                    and (
                    (((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                    cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                    and (cls.customer_data_dict.get("stop_eating_cheese") == "да"):
                cls.number_option = 13
                break

            # 14
            # Если запасы сыра равны нулю и уровень благосостояния покупателя средний/низкий,
            # текущая цена больше предыдущей, но рост цены превышает индивидуального порога чуствительности
            # при этом покупатель готов искать аналогичный товар у других производителей
            # найденная цена больше предыдущей, но рост цены превышает индивидуальный порог чуствительности
            # не готов переходить на более дешевый продукт
            # не готов отказаться от покупки продукта
            if (cls.balance_cheese == 0) \
                    and (cls.customer_data_dict.get('financial_wealth') != 'высокий') \
                    and (cls.retailer_current_price > cls.customer_last_price) \
                    and ((((cls.retailer_current_price - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                         cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("looking_cheese") == "да") \
                    and (cls.retailer_current_min_price_val > cls.customer_last_price) \
                    and (
                    (((cls.retailer_current_min_price_val - cls.customer_last_price) / cls.customer_last_price) * 100) > \
                    cls.customer_data_dict.get("significant_price_change")) \
                    and (cls.customer_data_dict.get("switching_another_cheese") == "нет") \
                    and (cls.customer_data_dict.get("stop_eating_cheese") == "нет"):
                cls.number_option = 14
                break

        # Номера соответствуют прилагаемой схеме принятия решения
        if cls.number_option in [2]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_VALUE_CHEESE if _ <= 500])
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [3, 8, 13]:
            pass

        elif cls.number_option in [4, 5, 6]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice(m.BUY_VALUE_CHEESE)
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [7, 12]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.retailer_current_cheaper_kind_cheese_val
            cls.customer_buy_dict['price'] = cls.retailer_current_cheaper_kind_cheese_price
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_VALUE_CHEESE if _ <= 500])
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [9, 14]:
            cls.customer_buy_dict['retailer'] = cls.customer_data_dict.get("basic_retailer")
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice([_ for _ in m.BUY_VALUE_CHEESE if _ <= 250])
            cls.customer_buy_dict['price'] = cls.retailer_current_price
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

        elif cls.number_option in [10, 11]:
            cls.customer_buy_dict['retailer'] = cls.retailer_current_min_price_retailer
            cls.customer_buy_dict['kind_cheese'] = cls.customer_data_dict.get("favorite_kind_cheese")
            cls.customer_buy_dict['quantity'] = random.choice(m.BUY_VALUE_CHEESE)
            cls.customer_buy_dict['price'] = cls.retailer_current_min_price_val
            cls.balance_cheese = cls.balance_cheese + cls.customer_buy_dict.get('quantity')

    @classmethod
    def update_customer_stocks(cls, dict_stock: Dict, customer_id: int) -> None:
        """Обновляем данные о товарном запасе покупателя"""
        dict_stock[customer_id] = cls.balance_cheese
        cls.balance_cheese = None

    @classmethod
    def process_purchase(cls, dt, model: int, customer_id: int) -> None:
        """Записать данные о покупке в датасет"""
        if cls.customer_buy_dict != {}:
            sale_record = (dt,
                           model,
                           cls.customer_buy_dict.get('retailer'),
                           customer_id,
                           cls.customer_buy_dict.get('kind_cheese'),
                           cls.customer_buy_dict.get('quantity'),
                           cls.customer_buy_dict.get('price'))
            cls.list_buys.append(sale_record)

    @classmethod
    def update_customer_last_price(cls, dict_last_price: Dict, customer_id: int) -> None:
        """Обновляем данные о последней цене покупки"""
        if cls.customer_buy_dict != {}:
            dict_last_price[customer_id] = cls.customer_buy_dict.get('price')
            cls.customer_buy_dict.clear()

    @classmethod
    def write_sales_csv(cls, model: int) -> None:
        """Записать данные о продажах в файл формата csv в рамках текущей модели"""
        list_col = ['date', 'model', 'retailer', 'customer_id', 'kind_cheese', 'quantity', 'price']
        df = pd.DataFrame(cls.list_buys, columns=list_col)
        if not os.path.exists(path_sales):
            os.mkdir(path_sales)
        full_path_file = os.path.join(path_sales, m.FILE_NAME_PATTERN_SALES.format(model))
        df.to_csv(full_path_file, sep=',', index=False)
        print('Запись файла с продажами завершена')
        cls.list_buys.clear()
