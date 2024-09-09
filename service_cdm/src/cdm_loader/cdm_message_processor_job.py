import time
from datetime import datetime
from logging import Logger
from confluent_kafka import Consumer
from confluent_kafka import Producer
import redis
import redis.client
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.repository.cdm_repository import CdmRepositoryBuilder
from typing import Dict, Optional, List
import json



class CdmMessageProcessor:
    def __init__(self,
                 KafkaConsumer: Consumer,
                 KafkaProducer: Producer,
                 RedisClient: redis.client,
                 CdmRepository: CdmRepository,
                 int:int,
                 logger: Logger) -> None:

        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._redis = RedisClient
        self._smd_repository = CdmRepository
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

            val =  json.loads(msg.value().decode())
            message = val['payload']

            #Формируем сообщения и вставляем в БД
            cdmRepositoryBuilder = CdmRepositoryBuilder(message)
            
            self._smd_repository.user_category_counters_insert(cdmRepositoryBuilder.user_category_counters())
            self._smd_repository.user_product_counters_insert(cdmRepositoryBuilder.user_product_counters())
            
            self._logger.info(f"{datetime.utcnow()}: CDM updated")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
