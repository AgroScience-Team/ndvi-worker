from typing import Dict

from ioc.anotations.beans.component import Component
from ioc.anotations.proxy.log.log import Log
from ioc.anotations.proxy.scheduled.kafka_listener.kafka_listener import KafkaListener
from ioc.kafka.consumers.consumer_record import ConsumerRecord
from ioc.kafka.producers.producer import Producer
from src.domain.listener.abstract_listener import Listener
from src.domain.workers.abstract_worker import Worker
from src.domain.workers.result import Result
from src.infra.audit.audit import Audit


@Component()
class NdviWorkerListener(Listener):
    def __init__(self, workers: list[Worker], producer: Producer):
        self.workers: Dict[str, Worker] = {worker.get_my_key(): worker for worker in workers}
        self._producer: Producer = producer

    @Audit("ndvi-worker", "agroscienceteam.audit.messages")
    @Log()
    @KafkaListener("ndvi-worker", "ndvi")
    def listen(self, message: ConsumerRecord):
        worker: Worker = self.workers.get(message.key)
        if worker is not None:
            self.workers.get(message.key).process(message.value)
        else:
            res: Result = Result(photoId=message.value, result="not supported", extension=message.key)
            self._producer.produce(self._result_topic, "ndvi", res.json())
