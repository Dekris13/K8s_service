import time
from datetime import datetime
from logging import Logger
from confluent_kafka import Consumer
from confluent_kafka import Producer
import redis
import redis.client
from dds_loader.repository.dds_repository import DdsRepository
from dds_loader.repository.dds_repository import DdsRepositoryBuilder
from dds_loader.repository.dds_repository import OutputMessageBuilder
from typing import Dict, Optional, List
import json



class DdsMessageProcessor:
    def __init__(self,
                 KafkaConsumer: Consumer,
                 KafkaProducer: Producer,
                 RedisClient: redis.client,
                 DdsRepository: DdsRepository,
                 DdsRepositoryBuilder: DdsRepositoryBuilder,
                 int:int,
                 logger: Logger) -> None:

        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._redis = RedisClient
        self._dds_repository = DdsRepository
        self._dds_builder_repository = DdsRepositoryBuilder
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

            msg.value().decode()
            val =  json.loads(msg.value().decode())
            message = val['payload']

            # Вставляем данные в БД DDS

            self._dds_builder_repository = DdsRepositoryBuilder(message)

            self._dds_repository.h_user_insert(self._dds_builder_repository.h_user())
            self._dds_repository.h_category_insert(self._dds_builder_repository.h_category())
            self._dds_repository.h_order_insert(self._dds_builder_repository.h_order())
            self._dds_repository.h_product_insert(self._dds_builder_repository.h_product())
            self._dds_repository.h_restaurant_insert(self._dds_builder_repository.h_restaurant())
           

            self._dds_repository.s_order_cost_insert(self._dds_builder_repository.s_order_cost())
            self._dds_repository.s_order_status_insert(self._dds_builder_repository.s_order_status())
            self._dds_repository.s_product_names_insert(self._dds_builder_repository.s_product_names())
            self._dds_repository.s_restaurant_names_insert(self._dds_builder_repository.s_restaurant_names())
            self._dds_repository.s_user_names_insert(self._dds_builder_repository.s_user_names())

            self._dds_repository.l_order_product_insert(self._dds_builder_repository.l_order_product())
            self._dds_repository.l_order_user_insert(self._dds_builder_repository.l_order_user())
            self._dds_repository.l_product_category_insert(self._dds_builder_repository.l_product_category())
            self._dds_repository.l_product_restaurant_insert(self._dds_builder_repository.l_product_restaurant())


        # Раздел. Отправтка данных в КАФКУ.
        # Получаем перечеть сообщений к отправке
        output_messages = OutputMessageBuilder(message).message_builder()

        #Отправляем сообщения
        for mes in output_messages:
            dst_msg = {"payload":mes.__dict__}
            
            # Отправляем ссобщение в кафку
            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
