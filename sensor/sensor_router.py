from sqlalchemy.orm import Session
from database import get_db

from fastapi import APIRouter,Depends

from sensor import sensor_crud,sensor_schema

from mqtt import mqtt_service
from mqtt import mqtt_service
import time

app=APIRouter(
    prefix="/sensor"
)

@app.post(path="/create",description="센서 생성")
async def create_new_sensor(new_sensor : sensor_schema.NewSensor,db:Session=Depends(get_db)):
    sensorId = sensor_crud.insert_sensor(new_sensor,db)
    sensorTopic=sensor_crud.get_sensor_topic(sensorId,db)
    #sensorTopic='my-topic'
    mqtt_service.create_mqtt_client(sensorTopic)
    return sensorId

@app.get(path="/read",description="모든 센서 조회")
async def read_all_sensor(db:Session=Depends(get_db)):
    return sensor_crud.list_all_sensor(db)


@app.get(path="/read/{sensorId}",description="특정 센서 조회")
async def read_sensor(sensorId : int,db:Session=Depends(get_db)):
    return sensor_crud.get_sensor(sensorId,db)

@app.put(path="/update/{sensorId}",description="특정 센서 수정")
async def update_sensor(sensorId: int,update_sensor : sensor_schema.UpdateSensor,db:Session=Depends(get_db)):
    return sensor_crud.update_sensor(sensorId,update_sensor,db)


@app.patch(path="/delete/{sensorId}",description="특정 센서 삭제")
async def delete_sensor_yn(sensorId : int,db:Session=Depends(get_db)):
    #sensorId = sensor_crud.insert_sensor(new_sensor,db)
    #sensorTopic=sensor_crud.get_sensor_topic(sensorId,db)
    sensorTopic='my-topic'
    mqtt_service.terminate_and_disconnect_client(sensorTopic)
    return sensor_crud.alter_del_yn(sensorId,db)


