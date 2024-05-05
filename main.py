
"""Fast API
uvicorn app:app --host 0.0.0.0 --port 8080
pip install pymysql
pip install fastapi
pip install pydantic
pip install SQLAlchemy
pip install mysql
pip install paho-mqtt"""

from fastapi import FastAPI, Depends, Path, HTTPException
import models 
from database import engine
from fastapi.middleware.cors import CORSMiddleware

models.Base.metadata.create_all(bind=engine)

from sensor import sensor_router

app=FastAPI()

app.include_router(sensor_router.app,tags=["sensor"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"hello" : "world"}


