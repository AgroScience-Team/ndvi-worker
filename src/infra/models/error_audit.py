from src.infra.models.audit_entity import AuditEntity


class ErrorAudit(AuditEntity):
    cause: str
    exceptionMessage: str
