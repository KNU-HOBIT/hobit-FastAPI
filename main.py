
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import models 
from database import engine

from sensor import sensor_router
from dataset import dataset_router

from request.request_router import router as request_router
from spark.spark_router import router as spark_router
from tp_chunk.chunk_router import router as chunk_router


app=FastAPI()




app.include_router(sensor_router.app,tags=["sensor"])
app.include_router(dataset_router.app,tags="dataset")
app.include_router(request_router)
app.include_router(spark_router)
app.include_router(chunk_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8899","http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"hello" : "world!!!"}


