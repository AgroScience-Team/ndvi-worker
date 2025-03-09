from typing import Optional

from pydantic import BaseModel


class Result(BaseModel):
    photoId: str
    jobId:str
    workerName: str
    path: Optional[str]
    success: bool
    type: str

def result_factory(message, object_name: Optional[str], success: bool, result_type: str) -> Result:
    return Result(
        photoId=message.photoId,
        jobId=message.jobId,
        workerName="ndvi-worker",
        path=object_name,
        success=success,
        type=result_type
    )
