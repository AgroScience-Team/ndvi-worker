import functools
import inspect
import sys


from ioc.anotations.proxy.proxy_configurator import ProxyConfigurator
from ioc.kafka.producers.producer import Producer
from src.infra.models.audit_entity import AuditEntity
from src.infra.models.audit_with_result import AuditEntityWithResult
from src.infra.models.error_audit import ErrorAudit
from src.infra.models.fatal_audit import FatalAudit


class AuditProxyConfigurator(ProxyConfigurator):
    def get_my_order(self) -> int:
        return sys.maxsize - 100

    def configure_if_needed(self, obj, application_context):
        obj_class = obj.__class__

        methods = inspect.getmembers(obj_class, predicate=inspect.isfunction)
        has_audit_methods = any(getattr(method, "_is_audit", False) for _, method in methods)

        if not has_audit_methods:
            return obj
        for method_name, method in methods:
            if getattr(method, "_is_audit", False):
                wrapped_method = self.create_audit_wrapper(method_name, method, application_context)
                setattr(obj_class, method_name, wrapped_method)

        return obj

    def create_audit_wrapper(self, method_name, method, application_context):

        producer: Producer = application_context.get_bean(Producer)
        audit_topic = method.audit_topic
        system = method.system_name
        success = "SUCCESS"
        error = "ERROR"

        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):

            try:
                result = method(self, *args, **kwargs)

                if result is None:
                    producer.produce(
                        audit_topic,
                        success,
                        AuditEntity(
                            system=system,
                            activeClass=str(self.__class__),
                            activeMethod=str(method_name),
                            params=args
                        ).json()
                    )
                else:
                    with_result = AuditEntityWithResult(
                        system=system,
                        activeClass=str(self.__class__),
                        activeMethod=method_name,
                        params=args,
                        result=str(result)
                    )
                    producer.produce(audit_topic, success, with_result.json())

                return result

            except Exception as e:
                try:
                    error_audit = ErrorAudit(
                        system=system,
                        activeClass=str(self.__class__),
                        activeMethod=method_name,
                        params=args,
                        cause=str(e.__class__),
                        exceptionMessage=str(e)
                    )
                    producer.produce(audit_topic, error, error_audit.json())
                except Exception as e:
                    producer.produce(
                        audit_topic,
                        error,
                        FatalAudit(
                            system=system,
                            cause=str(e.__class__),
                            exceptionMessage=str(e)
                        ).json()
                    )
                    raise e
                raise e

        return wrapped
