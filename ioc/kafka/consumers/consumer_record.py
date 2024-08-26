from pydantic import BaseModel


class ConsumerRecord(BaseModel):
    key: str
    value: str
