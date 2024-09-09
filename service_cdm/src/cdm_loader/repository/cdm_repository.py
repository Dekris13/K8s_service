
from lib.pg import PgConnect
from pydantic import BaseModel
import uuid


class user_category_counters(BaseModel):
    user_id:uuid.UUID
    category_id:uuid.UUID
    category_name:str

class user_product_counters(BaseModel):
    user_id:uuid.UUID
    product_id:uuid.UUID
    product_name:str

class CdmRepositoryBuilder:

    def __init__(self, payload: dict) -> None:
        self.dict = payload

    def user_category_counters(self) -> user_category_counters:
        return user_category_counters(
            user_id = self.dict['user_id'],
            category_id = self.dict['category_id'],
            category_name = self.dict['category_name']
        )
    
    def user_product_counters(self) -> user_product_counters:
        return user_product_counters(
            user_id = self.dict['user_id'],
            product_id = self.dict['product_id'],
            product_name = self.dict['product_name']
        )

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_insert(self, insert_class:object) -> None:

        with self._db.connection().cursor() as cur: 

            # Вставляем данные в витрину, если строка по user_id, category_id, category_name отсутсвует вставляем ее с order_cnt равном 1
            # если строка существует увеличиваем order_cnt на 1 единицу
            cur.execute(
                    """
                        insert into cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                        values (%(user_id)s,%(category_id)s,%(category_name)s,
                        coalesce(
                        (select order_cnt from cdm.user_category_counters 
                        where user_id = %(user_id)s and category_id = %(category_id)s and category_name = %(category_name)s), 1)
                        )
                        ON CONFLICT (user_id, category_id, category_name) DO UPDATE
                        SET
                            order_cnt = EXCLUDED.order_cnt + 1
                    """,
                        insert_class.__dict__
                )

    def user_product_counters_insert(self, insert_class:object) -> None:

        with self._db.connection().cursor() as cur: 

            # Вставляем данные в витрину, если строка по user_id, product_id, product_name отсутсвует вставляем ее с order_cnt равном 1
            # если строка существует увеличиваем order_cnt на 1 единицу
            cur.execute(
                    """
                        insert into cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                        values (%(user_id)s,%(product_id)s,%(product_name)s,
                        coalesce(
                        (select order_cnt from cdm.user_product_counters 
                        where user_id = %(user_id)s and product_id = %(product_id)s and product_name = %(product_name)s), 1)
                        )
                        ON CONFLICT (user_id, product_id, product_name) DO UPDATE
                        SET
                            order_cnt = EXCLUDED.order_cnt + 1
                    """,
                        insert_class.__dict__
                )
