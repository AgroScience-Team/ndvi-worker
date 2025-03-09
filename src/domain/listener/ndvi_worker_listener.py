import json
import os
from typing import Dict

from ioc.anotations.beans.component import Component
from ioc.anotations.proxy.log.log import Log
from ioc.anotations.proxy.scheduled.kafka_listener.kafka_listener import KafkaListener
from ioc.common_logger import log
from ioc.kafka.consumers.consumer_record import ConsumerRecord
from ioc.kafka.producers.producer import Producer
from src.domain.listener.abstract_listener import Listener
from src.domain.listener.not_supported_exception import NotSupportedException
from src.domain.models.worker_input import WorkerInput
from src.domain.workers.abstract_worker import Worker
from src.domain.models.result import Result, result_factory
from src.infra.audit.audit import Audit


@Component()
class NdviWorkerListener(Listener):
    def __init__(self, workers: list[Worker], producer: Producer):
        self.workers: Dict[str, Worker] = {worker.get_my_key(): worker for worker in workers}
        self._producer: Producer = producer
        self._result_topic = "agro.workers.results"

    @Audit("ndvi-worker", "agro.audit.messages")
    @Log()
    @KafkaListener("ndvi-worker", "agro.workers.ndvi")
    def listen(self, message: ConsumerRecord):
        input: WorkerInput = WorkerInput(**json.loads(message.value))
        worker: Worker = self.workers.get(input.extension)

        if worker is not None:
            self.workers.get(input.extension).process(input)
        else:
            log.warn(f"No worker with extension {input.extension}")
            res: Result = result_factory(message, None, False, "ndvi-preview")
            self._producer.produce(self._result_topic, "", res.json())
            res: Result = result_factory(message, None, False, "ndvi")
            self._producer.produce(self._result_topic, "", res.json())
            raise NotSupportedException(f"File with extension {input.extension} not supported")
