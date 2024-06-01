from sqlalchemy.orm import Session
from database import get_db

from fastapi import APIRouter,Depends,Query
from tp_chunk.chunk_crud import list_all_chunk
from tp_chunk.chunk_schema import ChunkReadReq
from starlette.responses import JSONResponse


router=APIRouter(
    prefix="/chunk"
)
# Update the endpoint to use query parameters
@router.get("/read/", description="특정 bucket, measurement, tag_key, tag_value 에 해당하는 모든 chuck 조회.")
async def data_by_time_range(
    bucket: str = Query(...),
    measurement: str = Query(...),
    tag_key: str = Query(...),
    tag_value: str = Query(...),
    send_topic: str = Query(...),
    db: Session = Depends(get_db)
):
    req = ChunkReadReq(
        bucket=bucket,
        measurement=measurement,
        tag_key=tag_key,
        tag_value=tag_value,
        send_topic=send_topic
    )
    result = list_all_chunk(req, db)
    return JSONResponse(content=result)
    