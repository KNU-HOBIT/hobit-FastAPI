import paho.mqtt.client as mqtt
import threading
import paho.mqtt.client as mqtt
import sys
import time
import random
from queue import Queue
import proto.hobit_pb2 as hobit_pb2
import json
import yaml
from google.protobuf import json_format

# config.yaml 파일에서 설정 로드
with open('./config/config.yaml', 'r') as stream:
    try:
        config = yaml.safe_load(stream)
        mqtt_broker_address = config['spark_config']['mqtt_broker_address']
        mqtt_port = config['spark_config']['mqtt_port']
        mqtt_username = config['spark_config']['mqtt_username']
        mqtt_password = config['spark_config']['mqtt_password']
    except yaml.YAMLError as exc:
        print(exc)

# 나머지 코드에서 사용할 수 있도록 설정값 할당
broker_address = mqtt_broker_address
port = mqtt_port
username = mqtt_username
password = mqtt_password
topic_to_client_map = {}
topic_to_thread_map = {}
message_queue = Queue()  # 스레드 간 통신을 위한 큐

def convert_proto_to_string(transport):
    # transport 객체의 속성들을 문자열로 변환하여 반환하는 함수
    attributes = [
        f"{key}: {getattr(transport, key)}"
        for key in transport.DESCRIPTOR.fields_by_name.keys()
    ]
    return "{" + ", ".join(attributes) + "}"


def on_message(client, userdata, message):
    # Add a delay to simulate processing time (optional)
    time.sleep(2)

    try:
        # Decode the Protocol Buffer message
        transport = hobit_pb2.Transport()
        
        transport.ParseFromString(message.payload)

        message_string = json_format.MessageToJson(transport)

        message_string = message_string.replace('nan', 'null')

        json_message = json.loads(message_string)

        json_message['message'] = message_string

        final_json_message = json.dumps(json_message)

        message_queue.put(final_json_message)

        print("디코딩된 프로토콜 버퍼 메시지를 JSON 형식으로 변환하여 메시지 큐에 추가했습니다.", final_json_message)
        print("메시지 큐 크기 확인:", message_queue.qsize())
    except Exception as e:
        print("예외가 발생했습니다:", e)
        
        
def create_mqtt_client(sensorTopic):
    
    client = mqtt.Client()
    # 유저 이름과 비밀번호 설정
    client.username_pw_set(username, password)
    
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("MQTT 클라이언트가 브로커에 성공적으로 연결되었습니다.")
        else:
            print(f"MQTT 클라이언트가 브로커에 연결하는 동안 오류가 발생했습니다. 코드: {rc}")

    client.on_connect = on_connect
    client.on_message = on_message

    # MQTT 브로커에 연결
    client.connect(broker_address, port, 60)

    client.subscribe(sensorTopic)
    thread = threading.Thread(target=client.loop_forever)
    thread.start()
    topic_to_thread_map[sensorTopic] = thread
    topic_to_client_map[sensorTopic] = client

def sendDataStreaming():
    time.sleep(1)
    while True:
        try:
            # 메시지 큐에서 데이터 꺼내기 (이벤트 기다림)
            message = message_queue.get(block=True, timeout=1)
            yield f"data: {message}\n\n"
            #return message
        except Exception as e:
            time.sleep(1)
            # 큐가 비어 있으면 2초 대기 후 다시 시도
            return "현재 큐가 비어있는 상태"

def start_thread(sensorTopic):
    create_mqtt_client(sensorTopic)

def get_client_for_topic(sensorTopic):
    return topic_to_client_map.get(sensorTopic)

def terminate_and_disconnect_client(sensorTopic):
    client = get_client_for_topic(sensorTopic)
    thread = topic_to_thread_map.get(sensorTopic)
    
    if client and thread:
        # 1. 루프 정지
        client.loop_stop()  # 강제로 루프 정지

        # 2. 구독 해제
        client.unsubscribe(sensorTopic)

        # 3. 연결 끊기
        client.disconnect()

        # 4. 스레드 종료
        if thread.is_alive():
            thread.join()
        # 4. 클라이언트 및 스레드 정보 삭제
        del topic_to_client_map[sensorTopic]
        del topic_to_thread_map[sensorTopic]    
