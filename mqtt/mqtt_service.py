import paho.mqtt.client as mqtt
import threading
import paho.mqtt.client as mqtt
import sys
import time
import random
from queue import Queue
# MQTT 설정
broker_address = '155.230.34.51'  # MQTT 브로커 주소
port = 30083              # MQTT 포트 (기본값은 1883)
username = 'admin'        # MQTT 유저 이름
password = 'password123'  # MQTT 비밀번호

topic_to_client_map = {}
topic_to_thread_map = {}
message_queue = Queue()  # 스레드 간 통신을 위한 큐

def on_message(client, userdata, message):
    time.sleep(5)
    print("message received ", str(message.payload.decode("utf-8")))
    print("message topic= ", message.topic)
    print("message qos=", message.qos)
    print("message retain flag= ", message.retain)
    
    message_queue.put(message.payload.decode("utf-8"))
    print("메세지 큐 크기 확인")
    print(message_queue.qsize())


def create_mqtt_client(sensorTopic):
    # MQTT Producer (Client) 인스턴스 생성
    client = mqtt.Client()
    # 유저 이름과 비밀번호 설정
    client.username_pw_set(username, password)
    client.on_message=on_message
    # MQTT 브로커에 연결
    client.connect(broker_address, port, 60)

    client.subscribe(sensorTopic)  
    thread = threading.Thread(target=client.loop_forever)
    thread.start()
    topic_to_thread_map[sensorTopic] = thread


def start_thread(sensorTopic):
    create_mqtt_client(sensorTopic)

def get_client_for_topic(sensorTopic):
    return topic_to_client_map.get(sensorTopic)

def terminate_and_disconnect_client(sensorTopic):
    client = get_client_for_topic(sensorTopic)
    thread = topic_to_thread_map.get(sensorTopic)


async def receive_mqtt_data():    
    time.sleep(5)
    while True:
        try:
            # 메시지 큐에서 데이터 꺼내기 (이벤트 기다림)
            message = message_queue.get(block=True, timeout=0.1)
            print("receive_mqtt_data 테스트 1")
            return message
        except Exception as e:
            time.sleep(5)
            print("receive_mqtt_data 테스트 2")
            # 큐가 비어 있으면 0.5초 대기 후 다시 시도
            return None