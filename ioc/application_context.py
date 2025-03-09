import importlib
import inspect
import pkgutil
from typing import Type, Dict

from ioc.anotations.proxy.proxy_configurator import ProxyConfigurator
from ioc.anotations.proxy.scheduled.scheduled import Scheduled
from ioc.common_logger import log
from ioc.object_factory import ObjectFactory


class ApplicationContext:

    def __init__(self, package_names: list[str]) -> None:
        self._object_factory = ObjectFactory(self)
        self._component_map: Dict[Type, list[Type]] = {}
        self._package_names = package_names
        self._package_names.append("ioc")
        self._cache: Dict[Type, list[Type]] = {}
        self._proxy_configurators: list[ProxyConfigurator] = []
        self._scheduled_beans: list[Scheduled] = []

    def _scan_for_components_and_configurators(self):
        if self._object_factory is None:
            raise Exception("Object factory is not injected")

        for package_name in self._package_names:
            package = importlib.import_module(package_name)
            # Обходим все модули в пакете и подпакетах
            for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
                module_name = module_info.name
                try:
                    # Импортируем модуль
                    module = importlib.import_module(module_name)
                    # Ищем все классы в модуле
                    for name, clazz in inspect.getmembers(module, inspect.isclass):
                        # Проверяем, имеет ли класс аннотацию component
                        if getattr(clazz, '_is_component', False):
                            # Ищем все базовые классы (интерфейсы)
                            for base in clazz.__bases__:
                                if inspect.isclass(base) and base is not object:
                                    # Добавляем класс в мапу по его базовому классу
                                    if self._component_map.get(base) is None:
                                        self._component_map[base] = []
                                    self._component_map[base].append(clazz)
                        if issubclass(clazz, ProxyConfigurator) and clazz is not ProxyConfigurator:
                            # Создаем экземпляр класса и добавляем в список
                            self._proxy_configurators.append(clazz())

                except Exception as e:
                    log.error(f"Не удалось загрузить модуль {module_name}: {e}")

        self._object_factory.set_proxy_configurators(self._proxy_configurators)

    def get_bean(self, param_type):
        clazz_list = self._component_map.get(param_type)

        if clazz_list is None:
            raise Exception("Unknown bean expected")

        if len(clazz_list) > 1:
            raise Exception("More than one expected bean exists")

        bean = None
        if self._cache.get(param_type) is None:
            self._cache[param_type] = []
            bean = self._object_factory.create(clazz_list[0])
            for base in bean.__class__.__bases__:
                self._cache[base] = [bean]
        else:
            bean = self._cache.get(param_type)[0]

        return bean

    def get_bean_list(self, param_type):
        if self._cache.get(param_type) is None:
            self._cache[param_type] = []
            for clazz in self._component_map[param_type]:
                bean = self._object_factory.create(clazz)
                for base in bean.__class__.__bases__:
                    self._cache[base].append(bean)
            return self._cache.get(param_type)
        return self._cache[param_type]

    def add_scheduled_bean(self, bean):
        self._scheduled_beans.append(bean)

    def _schedule(self):
        while True:
            for scheduled_bean in self._scheduled_beans:
                try:
                    scheduled_bean.schedule()
                except Exception as e:
                    log.exception(f"Scheduling error {str(scheduled_bean.__class__)}: {str(e)}")

    def run(self):
        log.info("Run application context")
        self._scan_for_components_and_configurators()
        for clazz in self._component_map.keys():
            if len(self._component_map.get(clazz)) == 1:
                log.info(f"Created bean: {self.get_bean(clazz)}")
            else:
                log.info(f"Created beans: {self.get_bean_list(clazz)}")

        self._schedule()
