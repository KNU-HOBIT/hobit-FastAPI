from pydantic import BaseModel

class RequestParams(BaseModel):
    start: str
    end: str
    bucket: str
    eqp_id: int
    send_topic: str

# class ResponseData(BaseModel):
#     query_start_str: str
#     query_end_str: str
#     eqp_id_str: str
#     bucket_str: str
#     send_topic_str: str
#     send_start: int
#     send_complete: int
#     total_message_cnt: int