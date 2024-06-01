from pydantic import BaseModel

class RequestParams(BaseModel):
    start: str
    end: str
    bucket: str
    eqp_id: int
    send_topic: str
