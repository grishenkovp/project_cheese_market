from typing import List
import model as m
from typing import List, Dict
from db import PostgresDB as db
from sql import script_get_retailer_current_price, \
                script_get_retailer_cheaper_kind_cheese, \
               script_get_retailer_min_price


class Retailer():
    """Работа с торговыми сетями"""

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
    def generation_list_retailer_proportion(cls) -> List[str]:
        """Генерация списка выбора торговой сети"""
        return cls.generation_list(m.RETAILER_PROPORTION)


    @classmethod
    def get_retailer_current_price(cls,dt,model:str, basic_retailer: str,
                                   favorite_kind_cheese: str,
                                   script=script_get_retailer_current_price) -> int:
        """Получить актуальную цену по дате, ритейлеру, виду сыра"""
        current_price_val = db().get_retailer_current_price(script,
                                                         dt=dt,
                                                        model=model,
                                                         retailer=basic_retailer,
                                                         kind_cheese=favorite_kind_cheese)
        return current_price_val

    @classmethod
    def get_retailer_cheaper_kind_cheese(cls, dt,
                                         model:str,
                                         retailer:str,
                                         retailer_current_price:int,
                                         script=script_get_retailer_cheaper_kind_cheese):
        """Получить по указанном ритейлеру на заданную дату вид сыра, цена на который меньше или равна цене
        на текущий вид сыра"""

        current_dict = db().get_retailer_cheaper_kind_cheese(script,
                                                             dt=dt,
                                                             model=model,
                                                             retailer=retailer,
                                                             retailer_current_price=retailer_current_price)

        kind_cheese = current_dict.get("kind_cheese")
        price = current_dict.get("price")
        return kind_cheese, price

    @classmethod
    def get_retailer_min_price(cls,
                               dt,
                               model: str,
                               current_kind_cheese:str,
                               retailer_top_list:str,script=script_get_retailer_min_price):
        # Получить ритейлера с самой конкурентной ценой на текущий вид сыра
        current_dict = db().get_retailer_min_price(script,
                                                             dt=dt,
                                                             model=model,
                                                             current_kind_cheese=current_kind_cheese,
                                                             retailer_top_list=retailer_top_list)
        retailer = current_dict.get("retailer")
        price = current_dict.get("price")
        return retailer, price