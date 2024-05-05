from pydantic import BaseModel
from typing import Optional

class NewSensor(BaseModel):
    sensorName: str
    sensorType : str 
    sensorEqpId: str

class SensorList(BaseModel):
    sensorId : int
    sensorName: str
    sensorType : str
    sensorTopic : str 
    sensorEqpId: str

class Sensor(BaseModel):
    sensorName: str
    sensorType : str
    sensorTopic : str 
    sensorEqpId: str 

class UpdateSensor(BaseModel):
    sensorName: str
    sensorType : str
    sensorEqpId: str