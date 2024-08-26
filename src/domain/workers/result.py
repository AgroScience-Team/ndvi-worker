from pydantic import BaseModel


class Result(BaseModel):
    photoId: str  # uuid
    result: str   # bool
    extension: str
