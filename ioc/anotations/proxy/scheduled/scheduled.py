from abc import ABC, abstractmethod


class Scheduled(ABC):
    @abstractmethod
    def schedule(self):
        pass
