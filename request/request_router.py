
from fastapi import APIRouter, HTTPException
from request.request_crud import data_by_time_range_req, all_of_data_req, check_elapsed_req
from request.request_schema import RequestParams
router = APIRouter()

@router.get("/data-by-time-range/")
async def data_by_time_range(
    start: str, 
    end: str, 
    bucket: str, 
    measurement: str,
    tag_key: str,
    tag_value: str,
):
    responce_result = data_by_time_range_req( start, end, bucket, measurement, tag_key, tag_value )

    if all((responce_result.get("totalMessages", None), 
            responce_result.get("startTimeMillis", None), 
            responce_result.get("endTimeMillis", None))):
        return responce_result
    else:
        raise HTTPException(status_code=400, detail="Request failed")

@router.get("/all-of-data/")
async def data_by_time_range(
    bucket: str, 
    measurement: str,
    tag_key: str,
    tag_value: str,
):
    responce_result = all_of_data_req( bucket, measurement, tag_key, tag_value )

    if all((responce_result.get("totalMessages", None), 
            responce_result.get("startTimeMillis", None), 
            responce_result.get("endTimeMillis", None))):
        return responce_result
    else:
        raise HTTPException(status_code=400, detail="Request failed")