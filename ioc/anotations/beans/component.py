class Component:
    def __call__(self, clazz):
        clazz._is_component = True
        return clazz
