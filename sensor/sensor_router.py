from sqlalchemy.orm import Session
from database import get_db
from fastapi import APIRouter,Depends
from fastapi.responses import StreamingResponse
from sensor import sensor_crud,sensor_schema
from mqtt import mqtt_service
import time
from starlette.responses import JSONResponse

app=APIRouter(
    prefix="/sensor"
)


@app.get("/disconnectionStreamingData")
async def disconnection_data(db: Session = Depends(get_db)):

    # 1. DB에서 모든 센서 객체 가져오기
    sensors = sensor_crud.list_all_sensor_id(db)

    # 2. 리스트를 돌면서
    for sensor in sensors:
        sensorTopic = sensor_crud.get_sensor_topic(sensor.id, db)  # 센서 객체의 ID 사용
        mqtt_service.terminate_and_disconnect_client(sensorTopic)  # 스레드 종료

    return JSONResponse({"message": "연결 끊기 성공"})

@app.get("/getStreamingData")
async def read_data(db: Session = Depends(get_db)):

    # 1. DB에서 모든 센서 객체 가져오기
    sensors = sensor_crud.list_all_sensor(db)
    print(sensors)  # 센서 객체 리스트 출력 (테스트용)

    # 2. 리스트를 돌면서
    for sensor in sensors:
        sensorTopic = sensor_crud.get_sensor_topic(sensor.sensorId, db) # 센서 객체의 ID 사용
        mqtt_service.start_thread(sensorTopic)  # 스레드 시작

    return StreamingResponse(mqtt_service.sendDataStreaming(),media_type="text/event-stream")

@app.post(path="/create",description="센서 생성")
async def create_new_sensor(new_sensor : sensor_schema.NewSensor,db:Session=Depends(get_db)):
    sensorId = sensor_crud.insert_sensor(new_sensor,db)
    sensorTopic=sensor_crud.get_sensor_topic(sensorId,db)
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
    sensorTopic='my-topic'
    mqtt_service.terminate_and_disconnect_client(sensorTopic)
    return sensor_crud.alter_del_yn(sensorId,db)


