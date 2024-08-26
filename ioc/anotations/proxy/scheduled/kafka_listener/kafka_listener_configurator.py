import inspect
import sys

from ioc.anotations.proxy.proxy_configurator import ProxyConfigurator
from ioc.anotations.proxy.scheduled.kafka_listener.default_consumer import DefaultScheduledConsumer
from ioc.application_context import ApplicationContext
from ioc.kafka.kafka_conf import KafkaConf


class KafkaListenerProxyConfigurator(ProxyConfigurator):
    def get_my_order(self) -> int:
        return sys.maxsize

    def configure_if_needed(self, obj, application_context: ApplicationContext):
        obj_class = obj.__class__

        methods = inspect.getmembers(obj_class, predicate=inspect.isfunction)
        kafka_listener_methods = [
            (method_name, method) for method_name, method in methods
            if getattr(method, "_is_kafka_listener", False)
        ]

        if not kafka_listener_methods:
            return obj

        for method_name, method in kafka_listener_methods:
            scheduled_bean = DefaultScheduledConsumer(
                conf=application_context.get_bean(KafkaConf),
                obj=obj,
                method=method,
                topic=method.topic,
                group_id=method.group_id

            )
            application_context.add_scheduled_bean(scheduled_bean)

        return obj
