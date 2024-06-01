from sqlalchemy.orm import Session
from models import Sensor
from sensor.sensor_schema import NewSensor,SensorList,UpdateSensor,SensorListId

def insert_sensor(new_sensor : NewSensor,db:Session):
    sensor=Sensor(
        sensorName=new_sensor.sensorName,
        sensorType=new_sensor.sensorType,
        sensorEqpId=new_sensor.sensorEqpId,
        sensorTopic=new_sensor.sensorType +"/"+new_sensor.sensorEqpId
    )

    
    db.add(sensor)
    db.commit()
    return sensor.id

def list_all_sensor(db:Session):
    lists=db.query(Sensor).filter(Sensor.sensorDeleteYN=='Y').all()
    return [SensorList(sensorId=row.id,sensorName=row.sensorName,sensorTopic=row.sensorTopic,sensorType=row.sensorType,sensorEqpId=row.sensorEqpId) for row in lists]

def get_sensor(sensorId : int , db: Session):
    sensor = db.query(Sensor).filter(Sensor.id==sensorId,Sensor.sensorDeleteYN=='Y').first()
    return Sensor(sensorName=sensor.sensorName,sensorTopic=sensor.sensorTopic,sensorType=sensor.sensorType,sensorEqpId=sensor.sensorEqpId)

def get_sensor_topic(sensorId : int,db: Session):
    sensor = db.query(Sensor).filter(Sensor.id==sensorId,Sensor.sensorDeleteYN=='Y').first()
    return sensor.sensorTopic

def update_sensor(sensorId :int ,updateSensor : UpdateSensor,db:Session):
    sensor=db.query(Sensor).filter(Sensor.id==sensorId,Sensor.sensorDeleteYN=='Y').first()

    sensor.sensorName = updateSensor.sensorName
    sensor.sensorType= updateSensor.sensorType
    sensor.sensorEqpId =updateSensor.sensorEqpId
    sensor.sensorTopic = updateSensor.sensorType +"/" + updateSensor.sensorEqpId
    db.commit()
    db.refresh(sensor)
    return get_sensor(sensor.id,db)


def alter_del_yn(sensorId:int,db:Session):
    sensor=db.query(Sensor).filter(Sensor.id==sensorId,Sensor.sensorDeleteYN=='Y').first()

    sensor.sensorDeleteYN='N'
    db.commit()
    db.refresh(sensor)
    return {'msg':'삭제가 완료되었습니다.'}
