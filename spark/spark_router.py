from fastapi import FastAPI, Query,Depends
from database import get_db
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session

from spark.spark_schema import MLReq
from spark.spark_crud import do_ml
from starlette.responses import JSONResponse
import json

router = APIRouter()

@router.post("/train/")
async def train_model(
    bucket: str = Query(...), # 데이터셋 특정
    measurement: str = Query(...), # 데이터셋 특정
    tag_key: str = Query(...), # 데이터셋 특정
    tag_value: str = Query(...), # 데이터셋 특정
    start: str = Query(...), # 실제 데이터셋 기간 텀.
    end: str = Query(...), # 실제 데이터셋 기간 텀.
    send_topic: str = Query(...), 
    train_ratio: float = Query(...),
    n_estimators: int = Query(...),
    feature_option_list: str = Query(...), # 한 문자열에 ","로 구분자.
    label_option: str = Query(...),
    db: Session = Depends(get_db)
):
    req = MLReq(
        bucket=bucket,
        measurement=measurement,
        tag_key=tag_key,
        tag_value=tag_value,
        start=start,
        end=end,
        send_topic=send_topic,
        train_ratio=train_ratio,
        n_estimators=n_estimators,
        feature_option_list=feature_option_list,
        label_option=label_option,
    )

    result = do_ml(req, db)
    '''
    result = {
        "mse" : mse_eval.evaluate(prediction),
        "rmse" : rmse_eval.evaluate(prediction),
        "r2" : r2_eval.evaluate(prediction),
        "power_consumption_list" : finish_pdf[label_option].tolist(),
        "prediction_list" : finish_pdf['prediction'].tolist(),
    }
    '''
    return JSONResponse(content=result)