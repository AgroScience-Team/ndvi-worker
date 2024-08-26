from abc import abstractmethod


class ProxyConfigurator:
    @abstractmethod
    def configure_if_needed(self, obj, application_context):
        pass

    @abstractmethod
    def get_my_order(self) -> int:
        pass
