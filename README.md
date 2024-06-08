## Configuration File 
```
├── __pycache__
│   ├── app.cpython-311.pyc
│   ├── database.cpython-311.pyc
│   ├── main.cpython-311.pyc
│   └── models.cpython-311.pyc
├── config
│   ├── config.yaml
│   └── config_loader.py
├── database.py
├── dataset
│   ├── dataset_crud.py
│   ├── dataset_router.py
│   └── dataset_schema.py
├── dataset_crud.py
├── hobit.proto
├── hobit_pb2.py
├── main.py
├── models.py
├── mqtt
│   ├── __pycache__
│   │   ├── mqtt_router.cpython-311.pyc
│   │   ├── mqtt_service.cpython-311.pyc
│   │   └── test.cpython-311.pyc
│   └── mqtt_service.py
├── proto
│   ├── hobit.desc
│   ├── hobit.proto
│   └── hobit_pb2.py
├── request
│   ├── request_crud.py
│   ├── request_router.py
│   └── request_schema.py
├── sensor
│   ├── __pycache__
│   │   ├── sensor_crud.cpython-311.pyc
│   │   ├── sensor_router.cpython-311.pyc
│   │   └── sensor_schema.cpython-311.pyc
│   ├── sensor_crud.py
│   ├── sensor_router.py
│   └── sensor_schema.py
├── spark
│   ├── __init__.py
│   ├── chunk.py
│   ├── funcs.py
│   ├── ml.py
│   ├── spark_crud.py
│   ├── spark_init.py
│   ├── spark_router.py
│   ├── spark_schema.py
│   └── util.py
├── tp_chunk
│   ├── chunk_crud.py
│   ├── chunk_router.py
│   └── chunk_schema.py
└── ubuntu_requirements.txt
```

##Configuration
프로젝트의 설정 파일은 `config` 디렉토리에 위치하며, 다음과 같은 파일들로 구성됩니다:
├── config
│ ├── config.yaml
│ └── config_loader.py

해당 파일은 spark와 mqtt broker과 연결을 위한 설정 파일입니다.
```
spark_config:
  master_url: "스파크 URL"
  driver_host: "driver host"
  driver_bindAddress: "0.0.0.0"
  executor_instances: 3
  executor_cores: 12
  executor_memory: "24G"
  app_name: "FastAPI-Spark Integration"
  kafka_bootstrap_servers: "kafka_bootstrap_servers IP"
  kafka_jars: [
    "jars/kafka-clients-3.4.1.jar",
    "jars/slf4j-api-2.0.7.jar",
    "jars/snappy-java-1.1.10.3.jar",
    "jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "jars/spark-streaming-kafka-0-10_2.12-3.5.1.jar",
    "jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "jars/commons-pool2-2.11.1.jar",
    "jars/spark-protobuf_2.12-3.5.1.jar"
  ]
  mqtt_broker_address: "mqtt_broker_address" 
  mqtt_port: 30083              
  mqtt_username: 'admin'        
  mqtt_password: 'password'  
```

##Installation 

1.Clone the repository:
 git clone https://github.com/KNU-HOBIT/hobit-FastAPI.git

2.Install the dependencies:
 pip install -r ubuntu_requirements.txt

#Usage
서버를 실행하려면 아래 명령어를 사용해 main.py를 실행하면 됩니다. 
uvicorn main:app --host 0.0.0.0 --port 8080


##Protobuf

프로젝트는 효율적인 데이터 직렬화를 위해 Protocol Buffers (Protobuf)를 사용합니다.
프로토콜 버퍼 스키마는 hobit.proto 파일에 정의되어 있습니다.

프로토콜 버퍼 파일을 Python 코드로 컴파일하려면 아래 명령어를 실행하세요:

protoc --python_out=. ./hobit.proto



# Main Function

이 어플리케이션은 사용자 요구에 따라 IoT 서버와 HOBIT 모듈을 통해 IoT 데이터를 전송하고 가공하는 API를 제공합니다. 주로 다음과 같은 기능을 수행합니다:

1. **MySQL과 연동하여 센서 등록 및 삭제 등 수행**
   - 데이터베이스 연결을 위해 `SQLAlchemy`를 사용함
   - 센서 데이터를 관리하기 위한 CRUD (Create, Read, Update, Delete) 기능을 제공함
   - 각 센서는 고유의 ID와 토픽(topic)을 가지며, 이를 통해 데이터를 구독하고 관리함

2. **MQTT 브로커 구독 및 Protobuf 형태의 데이터 디코딩 후 SSE를 활용한 React로 데이터 전송**
   - `paho-mqtt` 라이브러리를 사용하여 MQTT 브로커와 통신함
   - 센서 데이터는 Protobuf 형태로 인코딩되어 전송되며, 이를 디코딩하여 JSON 형식으로 변환함
   - 디코딩된 데이터는 `message_queue`를 통해 실시간으로 SSE (Server-Sent Events)를 사용하여 클라이언트에 전송함

3. **Spark Cluster에 데이터 요청 및 수신**
   - react로부터 http 요청을 수신하고, 해당 요청의 파라미터를 활용하여 Spark Cluster 서버에 데이터를 요청하고 응답을 받음
   - 수신된 데이터는 특정 형식에 맞춰 JSON 응답으로 반환됨
   - 데이터를 요청하는 다양한 엔드포인트를 제공함

