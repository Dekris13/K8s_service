from datetime import datetime
import uuid
import hashlib

from lib.pg import PgConnect
from pydantic import BaseModel

class h_user (BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class h_restaurant (BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str   

class h_product (BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str   

class h_order (BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str  

class h_category (BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str   

class s_user_names (BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str  
    hk_user_names_hashdiff: uuid.UUID

class s_restaurant_names (BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str  
    hk_restaurant_names_hashdiff: uuid.UUID

class s_product_names (BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str  
    hk_product_names_hashdiff: uuid.UUID

class s_order_status (BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str  
    hk_order_status_hashdiff: uuid.UUID

class s_order_cost (BaseModel):
    h_order_pk: uuid.UUID
    cost:float
    payment:float
    load_dt: datetime
    load_src: str  
    hk_order_cost_hashdiff: uuid.UUID

class l_product_restaurant (BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk:uuid.UUID
    h_restaurant_pk:uuid.UUID
    load_dt: datetime
    load_src: str  

class l_product_category (BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk:uuid.UUID
    h_category_pk:uuid.UUID
    load_dt: datetime
    load_src: str  

class l_order_user (BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk:uuid.UUID
    h_user_pk:uuid.UUID
    load_dt: datetime
    load_src: str 

class l_order_product (BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk:uuid.UUID
    h_product_pk:uuid.UUID
    load_dt: datetime
    load_src: str 

class output_message(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    category_id: uuid.UUID
    category_name: str

class DdsRepositoryBuilder():
    def __init__(self, message:dict) -> None:
        self.dict = message
        self.load_src = 'Kafka'

    def uuid_gen(arg:list):
            
        val = str()
        for i in arg:
            val = val+ str(i)

        hash = hashlib.sha256(val.encode('utf-8'))
        return uuid.UUID(hash.hexdigest()[::2])
    
    def h_user(self) -> h_user:
        user_id = self.dict['user']['id']
        return h_user(
            h_user_pk=self.uuid_gen(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.load_src
        )
    
    def h_restaurant(self) -> h_restaurant:
        restaurant_id = self.dict['restaurant']['id']
        return h_restaurant(
            h_restaurant_pk=self.uuid_gen(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.load_src
        )

    def h_product(self) -> list[h_product]:
        products = []   
        for item in self.dict['products']:
            product_id = item['id']
            products.append(
                h_product(
                    h_product_pk=self.uuid_gen(product_id),
                    product_id=product_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.load_src
                )
            )
        return products

    def h_order(self) -> h_order:
        order_id = self.dict['id']
        order_dt = self.dict['date']
        return h_order(
            h_order_pk=self.uuid_gen(order_id),
            order_id=order_id,
            order_dt = order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.load_src
        )
    
    def h_category(self) -> list[h_category]:
        categorys = []   
        for item in self.dict['products']:
            categorys.append(
                h_category(
                    h_category_pk=self.uuid_gen(item['category']),
                    category_name=item['category'],
                    load_dt=datetime.utcnow(),
                    load_src=self.load_src
                )
            )
        return categorys

    def s_user_names(self) -> s_user_names:
        user_id = self.dict['user']['id']
        username=self.dict['user']['name']
        userlogin = self.dict['user']['login']
        return s_user_names(
            h_user_pk=self.uuid_gen(user_id),
            username=username,
            userlogin = userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.load_src,
            hk_user_names_hashdiff = self.uuid_gen(user_id, username, userlogin)
        )

    def s_restaurant_names(self) -> s_restaurant_names:
        restaurant_id = self.dict['restaurant']['id']
        restaurant_name=self.dict['restaurant']['name']
        return s_restaurant_names(
            h_restaurant_pk=self.uuid_gen(restaurant_id),
            name=restaurant_name,
            load_dt=datetime.utcnow(),
            load_src=self.load_src,
            hk_restaurant_names_hashdiff = self.uuid_gen(restaurant_id, restaurant_name)
        )

    def s_product_names(self) -> list[s_product_names]:
        products = []   
        for item in self.dict['products']:
            product_id = item['id']
            product_name = item['name']
            products.append(
                s_product_names(
                    h_product_pk=self.uuid_gen(product_id),
                    name=product_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.load_src,
                    hk_product_names_hashdiff = self.uuid_gen(product_id, product_name)
                    )
                )
        return products

    def s_order_status(self) -> s_order_status:
        order_id = self.dict['id']
        order_date = self.dict['date']
        order_status = self.dict['status']
        return s_order_status(
            h_order_pk=self.uuid_gen(order_id, order_date),
            status=order_status,
            load_dt=datetime.utcnow(),
            load_src=self.load_src,
            hk_order_status_hashdiff = self.uuid_gen(order_id, order_date, order_status)
        )

    def s_order_cost(self) -> s_order_cost:
        order_id = self.dict['id']
        order_date = self.dict['date']
        order_cost = self.dict['cost']
        order_payment = self.dict['payment']
        return s_order_cost(
            h_order_pk=self.uuid_gen(order_id, order_date),
            cost=order_cost,
            payment = order_payment,
            load_dt=datetime.utcnow(),
            load_src=self.load_src,
            hk_order_cost_hashdiff = self.uuid_gen(order_id, order_date, order_cost, order_payment)
        )


    def l_product_restaurant(self) -> list [l_product_restaurant]:
        links = []   
        for item in self.dict['products']:
            product_id = item['id']
            restaurant_id = self.dict['restaurant']['id']
            h_product_pk=self.uuid_gen(product_id),
            h_restaurant_pk = self.uuid_gen(restaurant_id),

            links.append(
                l_product_restaurant(
                hk_product_restaurant_pk=self.uuid_gen(h_product_pk, h_restaurant_pk),
                h_product_pk=h_product_pk,
                h_restaurant_pk = h_restaurant_pk,
                load_dt=datetime.utcnow(),
                load_src=self.load_src
                )
            )
        return links
            
    def l_product_category(self) -> list[l_product_category]:
        links = []   
        for item in self.dict['products']:
            product_id = item['id']
            category_name = item['category']
            h_product_pk=self.uuid_gen(product_id),
            h_category_pk = self.uuid_gen(category_name),

            links.append(
                l_product_category(
                hk_product_category_pk=self.uuid_gen(h_product_pk, h_category_pk),
                h_product_pk=h_product_pk,
                h_category_pk = h_category_pk,
                load_dt=datetime.utcnow(),
                load_src=self.load_src
                )
            )
        return links

    def l_order_user(self) -> l_order_user:
        order_id = self.dict['id']
        order_date = self.dict['date']
        user_id = self.dict['user']['id']
        h_order_pk=self.uuid_gen(order_id, order_date),
        h_user_pk = self.uuid_gen(user_id),
        return(
                l_order_user(
                hk_order_user_pk=self.uuid_gen(h_order_pk, h_user_pk),
                h_order_pk=h_order_pk,
                h_user_pk = h_user_pk,
                load_dt=datetime.utcnow(),
                load_src=self.load_src
                )
            )

    def l_order_product(self) -> list[l_order_product]:
        links = []   
        for item in self.dict['products']:
            order_id = self.dict['id']
            order_date = self.dict['date']
            product_id = item['id']
            h_order_pk=self.uuid_gen(order_id, order_date),
            h_product_pk = self.uuid_gen(product_id),

            links.append(
                l_order_product(
                hk_order_product_pk=self.uuid_gen(h_order_pk, h_product_pk),
                h_order_pk=h_order_pk,
                h_product_pk = h_product_pk,
                load_dt=datetime.utcnow(),
                load_src=self.load_src
                )
            )
        return links
    
class OutputMessageBuilder:
    def __init__(self, message:dict) -> None:
        self.dict = message

    def uuid_gen(arg:list):
            
        val = str()
        for i in arg: val = val+ str(i)

        hash = hashlib.sha256(val.encode('utf-8'))
        return uuid.UUID(hash.hexdigest()[::2])

    def message_builder(self) -> list[output_message]:

        messages = []
        for item in self.dict['products']:
            user_id = self.uuid_gen(self.dict['user']['id'])
            product_id = self.uuid_gen(item['id'])
            category_id = self.uuid_gen(item['category'])
            messages.append(
                    output_message(
                    user_id=user_id,
                    product_id=product_id,
                    product_name = item['name'],
                    category_id=category_id,
                    category_name=item['category'],
                    )
                )
        return messages

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        values (%(h_user_pk)s,%(user_id)s,%(load_dt)s,%(load_src)s)
                        ON CONFLICT (h_user_pk) DO UPDATE
                        SET
                            user_id = EXCLUDED.user_id, load_dt = EXCLUDED.load_dt, load_src = EXCLUDED.load_src
                    """,
                        insert_class.__dict__
                )

    def h_restaurant_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        values (%(h_restaurant_pk)s,%(restaurant_id)s,%(load_dt)s,%(load_src)s)
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                        SET
                            restaurant_id = EXCLUDED.restaurant_id, load_dt = EXCLUDED.load_dt, load_src = EXCLUDED.load_src
                    """,
                        insert_class.__dict__
                )
            
    def h_product_insert(self, insert_class: object) -> None:

        for item in insert_class:
            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.h_product (h_product_pk, product_id, load_dt, load_src)
                            values (%(h_product_pk)s,%(product_id)s,%(load_dt)s,%(load_src)s)
                            ON CONFLICT (h_product_pk) DO UPDATE
                            SET
                                product_id = EXCLUDED.product_id, load_dt = EXCLUDED.load_dt, load_src = EXCLUDED.load_src
                        """,
                        item.__dict__
                    )
            
    def h_order_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                        values (%(h_order_pk)s,%(order_id)s,%(order_dt)s,%(load_dt)s,%(load_src)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                            order_id = EXCLUDED.order_id, order_dt = EXCLUDED.order_dt, load_dt = EXCLUDED.load_dt, load_src = EXCLUDED.load_src
                    """,
                    insert_class.__dict__
                )

    def h_category_insert(self, insert_class: object) -> None:

        for item in insert_class:

            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.h_category (h_category, category_name, load_dt, load_src)
                            values (%(h_category)s,%(category_name)s,%(load_dt)s,%(load_src)s)
                            ON CONFLICT (h_category) DO UPDATE
                            SET
                                category_name = EXCLUDED.category_name, load_dt = EXCLUDED.load_dt, load_src = EXCLUDED.load_src
                        """,
                        item.__dict__
                    )

    def s_user_names_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                        values (%(h_user_pk)s,%(username)s, %(userlogin)s, %(load_dt)s,%(load_src)s,%(hk_user_names_hashdiff)s)
                        ON CONFLICT (hk_user_names_hashdiff) DO UPDATE
                        SET
                            load_dt = EXCLUDED.load_dt
                    """,
                    insert_class.__dict__
                )

    def s_restaurant_names_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                        values (%(h_restaurant_pk)s,%(name)s,%(load_dt)s,%(load_src)s,%(hk_restaurant_names_hashdiff)s)
                        ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                        SET
                            load_dt = EXCLUDED.load_dt
                    """,
                    insert_class.__dict__
                )


    def s_product_names_insert(self, insert_class: object) -> None:

        for item in insert_class:
            
            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                            values (%(h_product_pk)s,%(name)s,%(load_dt)s,%(load_src)s, %(hk_product_names_hashdiff)s)
                            ON CONFLICT (hk_product_names_hashdiff) DO UPDATE
                            SET
                                load_dt = EXCLUDED.load_dt
                        """,
                        item.__dict__
                    )

    def s_order_status_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                        values (%(h_order_pk)s,%(status)s,%(load_dt)s,%(load_src)s,%(hk_order_status_hashdiff)s)
                        ON CONFLICT (hk_order_status_hashdiff) DO UPDATE
                        SET
                            load_dt = EXCLUDED.load_dt
                    """,
                        insert_class.__dict__
                )
            
    def s_order_cost_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.s_order_cost (h_order_pk, cost ,payment, load_dt, load_src, hk_order_cost_hashdiff)
                        values (%(h_order_pk)s,%(cost)s,%(payment)s,%(load_dt)s,%(load_src)s,%(hk_order_cost_hashdiff)s)
                        ON CONFLICT (hk_order_cost_hashdiff) DO UPDATE
                        SET
                            load_dt = EXCLUDED.load_dt
                    """,
                        insert_class.__dict__
                )

    def l_order_product_insert(self, insert_class: object) -> None:

        for item in insert_class:

            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                            values (%(hk_order_product_pk)s,%(h_order_pk)s,%(h_product_pk)s,%(load_dt)s,%(load_src)s)
                            ON CONFLICT (hk_order_product_pk) DO UPDATE
                            SET
                                load_dt = EXCLUDED.load_dt
                        """,
                        item.__dict__
                    )
                
    def l_order_user_insert(self, insert_class: object) -> None:

        with self._db.connection().cursor() as cur: 
            cur.execute(
                    """
                        insert into dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                        values (%(hk_order_user_pk)s,%(h_order_pk)s,%(h_user_pk)s,%(load_dt)s,%(load_src)s)
                        ON CONFLICT (hk_order_user_pk) DO UPDATE
                        SET
                            load_dt = EXCLUDED.load_dt
                    """,
                        insert_class.__dict__
                )

    def l_product_category_insert(self, insert_class: object) -> None:

        for item in insert_class:

            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.l_product_category (hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                            values (%(hk_product_category_pk)s,%(h_product_pk)s,%(h_category_pk)s,%(load_dt)s,%(load_src)s)
                            ON CONFLICT (hk_product_category_pk) DO UPDATE
                            SET
                                load_dt = EXCLUDED.load_dt
                        """,
                            item.__dict__
                    )
                
    def l_product_restaurant_insert(self, insert_class: object) -> None:

        for item in insert_class:

            with self._db.connection().cursor() as cur: 
                cur.execute(
                        """
                            insert into dds.l_product_restaurant (hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_dt, load_src)
                            values (%(hk_product_restaurant_pk)s,%(h_product_pk)s,%(h_restaurant_pk)s,%(load_dt)s,%(load_src)s)
                            ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                            SET
                                load_dt = EXCLUDED.load_dt
                        """,
                            item.__dict__
                    )

