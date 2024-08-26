from pydantic import BaseModel


class FatalAudit(BaseModel):
    system: str
    cause: str
    exceptionMessage: str
