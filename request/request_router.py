from fastapi import APIRouter, HTTPException
from request.request_crud import get_request_data
from request.request_schema import RequestParams
from spark.spark_crud import run_spark_kafka_job
router = APIRouter()

@router.get("/send_request/")
async def send_request(params: RequestParams):
    _, _, _, _, _, total_messages, send_start_time, send_end_time = get_request_data(
        params.start, params.end, params.bucket, params.eqp_id, params.send_topic)

    run_spark_kafka_job(send_start_time, send_end_time, total_messages, params.send_topic)

    if all((total_messages, send_start_time, send_end_time)):
        return {
            "send_start": send_start_time,
            "send_complete": send_end_time,
            "total_message_cnt": total_messages
        }
    else:
        raise HTTPException(status_code=400, detail="Request failed")