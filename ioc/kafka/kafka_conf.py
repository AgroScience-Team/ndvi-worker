import os
from abc import ABC, abstractmethod

from kafka.admin import KafkaAdminClient

from ioc.anotations.beans.component import Component
from ioc.common_logger import log


class KafkaConf(ABC):
    @abstractmethod
    def get_kafka_user(self) -> str:
        pass

    @abstractmethod
    def get_kafka_password(self) -> str:
        pass

    @abstractmethod
    def get_kafka_bootstrap_servers(self) -> str:
        pass

    @abstractmethod
    def get_kafka_admin_client(self):
        pass


@Component()
class DefaultKafkaConf(KafkaConf):
    def __init__(self):
        self._KAFKA_USER = os.getenv('kafka.user')
        self._KAFKA_PASSWORD = os.getenv('kafka.password')
        self._KAFKA_BOOTSTRAP_SERVERS = os.getenv('kafka.bootstrap-servers')
        log.info(f"DefaultKafkaConf started {self._KAFKA_USER}|{self._KAFKA_PASSWORD}|{self._KAFKA_BOOTSTRAP_SERVERS}")
        self._kafka_admin_client = KafkaAdminClient(
            bootstrap_servers=self._KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=self._KAFKA_USER,
            sasl_plain_password=self._KAFKA_PASSWORD,
            client_id='my_admin_client'
        )
        log.info("KafkaAdminClient успешно создан")

    def get_kafka_user(self) -> str:
        return self._KAFKA_USER

    def get_kafka_password(self) -> str:
        return self._KAFKA_PASSWORD

    def get_kafka_bootstrap_servers(self) -> str:
        return self._KAFKA_BOOTSTRAP_SERVERS

    def get_kafka_admin_client(self):
        return self._kafka_admin_client
