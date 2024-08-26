class Audit:
    def __init__(self, system, audit_topic):
        self._audit_topic = audit_topic
        self._system_name = system

    def __call__(self, method):
        method._is_audit = True
        method.audit_topic = self._audit_topic
        method.system_name = self._system_name
        return method
