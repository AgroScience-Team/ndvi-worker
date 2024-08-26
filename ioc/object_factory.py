import inspect
from typing import get_type_hints, get_origin

from ioc.anotations.proxy.proxy_configurator import ProxyConfigurator


class ObjectFactory:
    def __init__(self, application_context) -> None:
        self._application_context = application_context
        self._proxy_configurators: list[ProxyConfigurator] = []

    def set_proxy_configurators(self, proxy_configurators: list[ProxyConfigurator]):
        proxy_configurators.sort(key=lambda configurator: configurator.get_my_order())
        self._proxy_configurators = proxy_configurators

    def create(self, clazz):
        constructor = clazz.__init__

        signature = inspect.signature(constructor)

        type_hints = get_type_hints(constructor)

        constructor_args = {}

        for name, parameter in signature.parameters.items():
            if name == 'self' or name == 'args' or name == 'kwargs':
                continue
            param_type = type_hints.get(name)

            if get_origin(param_type) == list:
                list_type = param_type.__args__[0]
                constructor_args[name] = self._application_context.get_bean_list(list_type)
            else:
                constructor_args[name] = self._application_context.get_bean(param_type)
        obj = clazz(**constructor_args)

        for configurator in self._proxy_configurators:
            obj = configurator.configure_if_needed(obj, self._application_context)
        return obj
