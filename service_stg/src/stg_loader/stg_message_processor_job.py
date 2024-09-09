import time
from datetime import datetime
from logging import Logger
from confluent_kafka import Consumer
from confluent_kafka import Producer
import redis
import redis.client
from stg_loader.repository.stg_repository import StgRepository
from typing import Dict, Optional, List
import json



class StgMessageProcessor:
    def __init__(self,
                 KafkaConsumer: Consumer,
                 KafkaProducer: Producer,
                 RedisClient: redis.client,
                 int:int,
                 logger: Logger) -> None:

        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._redis = RedisClient
        self._stg_repository = StgRepository
        self._batch_size = int
        self._logger = logger

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            order = msg['payload']
            self._stg_repository.order_events_insert(
                msg["object_id"],
                msg["object_type"],
                msg["sent_dttm"],
                json.dumps(order))

            user_id = order["user"]["id"]
            user = self._redis.get(user_id)
            user_name = user["name"]
            user_login = user["login"]
            
            restaurant_id = order['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant["name"]
            
            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name, user_login),
                    "products": self._format_items(order["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name) -> Dict[str, str]:
        return {
            "id": id,
            "name": name
        }

    def _format_user(self, id, name, login) -> Dict[str, str]:
        return {
            "id": id,
            "name": name,
            "login" : login
        }

    def _format_items(self, order_items, restaurant) -> List[Dict[str, str]]:
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items
