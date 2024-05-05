from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base

DB_URL = 'mysql+pymysql://root:knulinkmoa1234@localhost:3306/hobit'
Base = declarative_base()

class Sensor(Base):
    __tablename__ = 'sensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sensorName = Column(String(255), nullable=False)
    sensorTopic = Column(String(255), nullable=False)
    sensorType = Column(String(255), nullable=False)
    sensorEqpId=Column(String(255),nullable=False)
    sensorDeleteYN=Column(String(1),nullable=False,default='Y')


engine = create_engine(DB_URL)
Base.metadata.create_all(engine)
