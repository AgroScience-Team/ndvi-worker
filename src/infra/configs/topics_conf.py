from ioc.anotations.beans.component import Component
from ioc.kafka.kafka_conf import KafkaConf
from ioc.kafka.topics.new_topic import Topic


@Component()
class NdviTopic(Topic):

    def __init__(self, conf: KafkaConf):
        super().__init__(conf, "agro.workers.ndvi")


@Component()
class WorkersResultsTopic(Topic):

    def __init__(self, conf: KafkaConf):
        super().__init__(conf, "agro.workers.results")


@Component()
class AuditTopic(Topic):

    def __init__(self, conf: KafkaConf):
        super().__init__(conf, "agro.audit.messages")
