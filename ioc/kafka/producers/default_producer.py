from kafka import KafkaProducer

from ioc.anotations.beans.component import Component
from ioc.kafka.kafka_conf import KafkaConf
from ioc.kafka.producers.producer import Producer


@Component()
class DefaultProducer(Producer):

    def __init__(self, conf: KafkaConf) -> None:
        self._producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=conf.get_kafka_bootstrap_servers(),
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8') if v is not None else None,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username=conf.get_kafka_user(),
            sasl_plain_password=conf.get_kafka_password()
        )

    def produce(self, topic, key, value):
        self._producer.send(topic, value, key)
