from typing import Optional

from pydantic import BaseModel


class ConsumerRecord(BaseModel):
    key: Optional[str]
    value: str
