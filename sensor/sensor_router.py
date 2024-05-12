from sqlalchemy.orm import Session
from database import get_db

from fastapi import APIRouter,Depends
from fastapi.responses import StreamingResponse

from sensor import sensor_crud,sensor_schema

from mqtt import mqtt_service
from mqtt import mqtt_service
import time
from starlette.responses import JSONResponse

app=APIRouter(
    prefix="/sensor"
)


""" 
@app.get(path="/readData/{sensorId}")
async def read_data(sensorId : int ,db:Session=Depends(get_db)):
    sensorTopic=sensor_crud.get_sensor_topic(sensorId,db)
    mqtt_service.start_thread(sensorTopic)
    return StreamingResponse(mqtt_service.sendDataStreaming())
"""
@app.get(path="/DisconnectionStreamingData")
async def read_data(request : Request ,db:Session=Depends(get_db)):
    sensorIds = request.query_params.get("sensorIds")  # "sensorIds" 라는 키로 요청 파라미터 값 가져오기
    if sensorIds is not None:
        sensorIds = sensorIds.split(",")  # 쉼표(,)로 구분된 문자열을 리스트로 변환
        sensorIds = [int(sensorId) for sensorId in sensorIds]  # 문자열을 숫자로 변환

        # sensorIds 리스트를 반복하며 처리
        for sensorId in sensorIds:
            sensorTopic = sensor_crud.get_sensor_topic(sensorId, db)  # 각 센서 ID 기반으로 토픽 생성
            mqtt_service.terminate_and_disconnect_client(sensorTopic)  # 스레드 시작

    return JSONResponse({"message": "데이터 삭제 성공"})


@app.get(path="/getStreamingData")
async def read_data(request : Request ,db:Session=Depends(get_db)):
    sensorIds = request.query_params.get("sensorIds")  # "sensorIds" 라는 키로 요청 파라미터 값 가져오기
    if sensorIds is not None:
        sensorIds = sensorIds.split(",")  # 쉼표(,)로 구분된 문자열을 리스트로 변환
        sensorIds = [int(sensorId) for sensorId in sensorIds]  # 문자열을 숫자로 변환

        # sensorIds 리스트를 반복하며 처리
        for sensorId in sensorIds:
            sensorTopic = sensor_crud.get_sensor_topic(sensorId, db)  # 각 센서 ID 기반으로 토픽 생성
            mqtt_service.start_thread(sensorTopic)  # 스레드 시작

    return StreamingResponse(mqtt_service.sendDataStreaming())



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


