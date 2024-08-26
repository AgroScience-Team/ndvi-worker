from abc import ABC

from kafka.admin import KafkaAdminClient, NewTopic

from ioc.common_logger import log
from ioc.kafka.kafka_conf import KafkaConf


class Topic(ABC):
    def __init__(self, conf: KafkaConf, topic: str, num_partitions: int = 1, replication_factor: int = 1):
        self._conf = conf
        self._topic = topic
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor
        self._create_topic()

    def _create_topic(self):
        client: KafkaAdminClient = self._conf.get_kafka_admin_client()
        try:
            client.create_topics([NewTopic(
                name=self._topic,
                num_partitions=self._num_partitions,
                replication_factor=self._replication_factor
            )])
            log.info(f"Kafka topic {self._topic} created")
        except Exception as e:
            log.warn(f"{str(e)}")
