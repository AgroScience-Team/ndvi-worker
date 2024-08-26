from abc import abstractmethod, ABC


class Worker(ABC):
    @abstractmethod
    def process(self, message):
        pass

    @abstractmethod
    def get_my_key(self) -> str:
        pass
