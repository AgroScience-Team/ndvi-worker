from typing import Any

from pydantic import BaseModel


class AuditEntity(BaseModel):
    system: str
    activeClass: str
    activeMethod: str
    params: tuple[Any, ...]
