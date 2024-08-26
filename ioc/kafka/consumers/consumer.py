from abc import ABC, abstractmethod

from ioc.kafka.consumers.consumer_record import ConsumerRecord


class Consumer(ABC):
    @abstractmethod
    def consume(self) -> ConsumerRecord:
        pass
