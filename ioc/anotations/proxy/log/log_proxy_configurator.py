import functools
import inspect
import sys

from ioc.anotations.proxy.proxy_configurator import ProxyConfigurator
from ioc.common_logger import log


class LogProxyConfigurator(ProxyConfigurator):
    def get_my_order(self) -> int:
        return sys.maxsize - 200

    def configure_if_needed(self, obj, application_context):
        obj_class = obj.__class__

        methods = inspect.getmembers(obj_class, predicate=inspect.isfunction)
        has_log_methods = any(getattr(method, "_is_log", False) for _, method in methods)

        if not has_log_methods:
            return obj

        for method_name, method in methods:
            if getattr(method, "_is_log", False):
                wrapped_method = self.create_wrapped_method(method_name, method)
                setattr(obj_class, method_name, wrapped_method)

        return obj

    def create_wrapped_method(self, method_name, method):

        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):
            class_name = self.__class__.__name__
            log.info(f"Start method {class_name}.{method_name} with arguments: {args} {kwargs}")
            result = None
            try:
                result = method(self, *args, **kwargs)
            except Exception as e:
                log.error(f"End method with error {class_name}.{method_name}. Exception: {e}")
                raise e
            if result is not None:
                log.info(f"End method {class_name}.{method_name} returned: {result}")
            else:
                log.info(f"End method {class_name}.{method_name}")
            return result

        return wrapped
