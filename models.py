from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

DB_URL = 'mysql+pymysql://root:password123@155.230.34.51:30036/hobit'
Base = declarative_base()

class Sensor(Base):
    __tablename__ = 'sensor'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sensorName = Column(String(255), nullable=False)
    sensorTopic = Column(String(255), nullable=False)
    sensorType = Column(String(255), nullable=False)
    sensorEqpId=Column(String(255),nullable=False)
    sensorDeleteYN=Column(String(1),nullable=False,default='Y')


# Chunk table definition
class Chunk(Base):
    __tablename__ = 'chunk'
    bucket = Column(String(20), primary_key=True)
    measurement = Column(String(20), primary_key=True)
    tagKey = Column(String(20), primary_key=True)
    tagValue = Column(String(20), primary_key=True)
    startTs = Column(BigInteger, primary_key=True)
    endTs = Column(BigInteger)
    chunkDuration = Column(BigInteger)
    count = Column(Integer)

    __table_args__ = (
        UniqueConstraint('bucket', 'measurement', 'tagKey', 'tagValue', 'startTs', name='uix_1'),
    )

    def print_chunk(self):
        data = {key: value for key, value in vars(self).items() if key != '_sa_instance_state'}

        col_width = max(len(key) for key in data.keys()) + 2
        record_width = max(len(str(value)) for value in data.values()) + 2
        type_width = max(len(type(value).__name__) for value in data.values()) + 2

        total_width = col_width + record_width + type_width + 6

        print("=" * total_width)
        print(f"{'col':<{col_width}}|{'record':<{record_width}}|{'data type':<{type_width}}")
        print("=" * total_width)
        for key, value in data.items():
            print(f"{key:<{col_width}}|{str(value):<{record_width}}|{type(value).__name__:<{type_width}}")
        print("=" * total_width)

# Create an engine
engine = create_engine(DB_URL)

# Create all tables
Base.metadata.create_all(engine)