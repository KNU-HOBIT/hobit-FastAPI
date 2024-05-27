
from fastapi import APIRouter, HTTPException
from request.request_crud import get_request_data
from request.request_schema import RequestParams
from spark.spark_crud import run_spark_kafka_job
router = APIRouter()

@router.get("/data-by-time-range/")
async def data_by_time_range(
    start: str, 
    end: str, 
    bucket: str, 
    measurement: str,
    tag_key: str,
    tag_value: str,
    send_topic: str
):
    responce_result = get_request_data( start, end, bucket, measurement, tag_key, tag_value, send_topic )
    
    run_spark_kafka_job(responce_result.get("startTimeMillis", None), responce_result.get("endTimeMillis", None), responce_result.get("totalMessages", None), send_topic)

    if all((responce_result.get("totalMessages", None), 
            responce_result.get("startTimeMillis", None), 
            responce_result.get("endTimeMillis", None))):
        return responce_result
    else:
        raise HTTPException(status_code=400, detail="Request failed")
