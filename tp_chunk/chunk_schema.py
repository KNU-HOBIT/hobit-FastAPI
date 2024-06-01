from pydantic import BaseModel

class ChunkReadReq(BaseModel):
    bucket: str
    measurement: str
    tag_key: str
    tag_value: str
    send_topic: str