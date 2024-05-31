from sqlalchemy.orm import Session
from database import get_db

from fastapi import APIRouter,Depends
from tp_chunk.chunk_crud import list_all_chunk
from tp_chunk.chunk_schema import ChunkReadReq

router=APIRouter(
    prefix="/chunk"
)
@router.get(path="/read/", description="특정 bucket, measurement, tag_key, tag_value 에 해당하는 모든 chuck 조회.")
async def data_by_time_range(req :ChunkReadReq, db :Session=Depends(get_db)):
    return list_all_chunk(req, db)
    