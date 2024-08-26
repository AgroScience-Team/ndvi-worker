from abc import abstractmethod, ABC

from ioc.kafka.consumers.consumer_record import ConsumerRecord


class Listener(ABC):
    @abstractmethod
    def listen(self, message: ConsumerRecord):
        pass
