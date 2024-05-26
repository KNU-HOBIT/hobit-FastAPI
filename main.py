
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# from sensor import sensor_router
# from mqtt import mqtt_router
from request.request_router import router as request_router
from spark.spark_router import router as spark_router

app=FastAPI()

# app.include_router(sensor_router.app,tags=["sensor"])
# app.include_router(mqtt_router.app,tags=["mqtt"])

app.include_router(request_router)
app.include_router(spark_router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8899"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"hello" : "world!!!"}


