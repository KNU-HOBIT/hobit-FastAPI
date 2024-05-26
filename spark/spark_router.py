from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException

router = APIRouter()

@router.post("/train/")
async def train_model():
    # Spark 작업을 실행하는 함수 호출
    # result = run_spark_job()
    # return {"result": result}
    pass
