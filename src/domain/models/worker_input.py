from pydantic import BaseModel


class WorkerInput(BaseModel):
    jobId: str
    photoId: str
    extension: str
