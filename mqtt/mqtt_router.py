from fastapi import FastAPI, WebSocket, WebSocketDisconnect,APIRouter,Depends
from sqlalchemy.orm import Session
import paho.mqtt.client as mqtt
import threading
import asyncio
from mqtt import mqtt_service
import time
from database import get_db
from sensor import sensor_crud,sensor_schema

app=APIRouter()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    print("웹 소켓 통신 요청을 받았습니다.")
    await websocket.accept()  # 연결 수락

    try:
        while True:
            # 1. React 서버로부터 메시지 받기 (선택적)
            # data = await websocket.receive_text()
            # if data:
            #     # React 서버로부터 받은 데이터 처리 (필요에 따라 구현)
            #     pass

            # 2. MQTT 메시지 수신 대기
            # (MQTT 클라이언트 스레드에서 수신된 데이터를 받아옴)
            time.sleep(5)
            data = mqtt_service.receive_mqtt_data()
            print("데이터를 받았습니다.",data)
            await websocket.send_text(str(data))  # React 서버에 데이터 전송
            print("데이터를 받았습니다.2",data)
            # 3. 수신된 데이터 전송
            if data:
                print("데이터 존재함")
                print(data)
                try:
                    await websocket.send_text(str(data))  # React 서버에 데이터 전송
                except WebSocketDisconnect:
                    break                    

    except Exception as e:
        print(f"예외 발생: {e}")

