from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import httpx
from starlette.responses import JSONResponse

from dataset import dataset_schema



app=APIRouter(
    prefix="/dataset"
)
# IP 주소 :155.230.36.25
# port : 3001



@app.get("/type")
async def dataset_type():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://155.230.36.25:3001/bucket-list/")
        
        if response.status_code == 200:
            data = response.json()
            # 응답이 리스트인지 확인
            #if isinstance(data, list) and all(isinstance(item, dict) and 'bucket_name' in item and 'measurement' in item for item in data):
            return JSONResponse(content= data)    
            #else:
                #raise HTTPException(status_code=500, detail="Invalid response format: Expected a list of dictionaries with 'bucket_name' and 'measurement'")
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data from the server")
    
    except httpx.RequestError as exc:
        raise HTTPException(status_code=500, detail=f"An error occurred while requesting data: {exc}")
@app.get("/selection")
async def dataset_selection(dataSelection: dataset_schema.datasetSelection):
    try:
        # HTTP 요청 보내기 (쿼리 파라미터로 데이터 포함)
        async with httpx.AsyncClient() as client:
            url = f"http://155.230.36.25:3001/bucket-detail/?bucket_name={dataSelection.bucket_name}&measurement={dataSelection.measurement}"
            response = await client.get(url)
        
        # 요청 결과 반환
        if response.status_code == 200:
            data = response.json()
            # 데이터의 형식이 요구사항과 일치하는지 확인
            if isinstance(data, dict) and "bucket" in data and "columns" in data and "count" in data and "data" in data and "end" in data and "start" in data and "measurement" in data:
                result = {
                    "bucket_name": data["bucket"],
                    "columns": data["columns"],
                    "count": data["count"],
                    "data": data["data"][:2],  # 실제 데이터 예시 2개
                    "end": data["end"],
                    "start": data["start"],
                    "measurement": data["measurement"]
                }
                return JSONResponse(content=result)
            else:
                raise HTTPException(status_code=500, detail="Invalid response format: Expected a specific structure")
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data from the server")
    
    except httpx.RequestError as exc:
        raise HTTPException(status_code=500, detail=f"An error occurred while requesting data: {exc}")

@app.get("/option")
async def dataset_option(datasetOption : dataset_schema.datasetOption):
    try:
        # HTTP 요청 보내기 (쿼리 파라미터로 데이터 포함)
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "http://155.230.36.25:3001/your-option-endpoint",
                params= [("option",option) for option in datasetOption.option]
                #params=[("name", name) for name in dataSelection.name]
            )
        
        # 요청 결과 반환
        if response.status_code == 200:
            datasetReslut = response.json()
            # datasetReslut 리스트인지 확인
            if isinstance(datasetReslut, list):
                return JSONResponse(content={"datasetReslut": datasetReslut})
            else:
                raise HTTPException(status_code=500, detail="Invalid response format: Expected a list")
        else:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data from the server")
    
    except httpx.RequestError as exc:
        raise HTTPException(status_code=500, detail=f"An error occurred while requesting data: {exc}")