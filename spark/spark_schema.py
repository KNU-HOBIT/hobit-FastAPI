from pydantic import BaseModel

class MLReq(BaseModel):
    bucket: str # for 데이터셋 특정
    measurement: str # for 데이터셋 특정
    tag_key: str # for 데이터셋 특정
    tag_value: str # for 데이터셋 특정
    start: str # ex "2020-09-01T00:00:00Z" 또는 문자열로 cast한 밀리초단위 Unixtimestampe ex "1627607610302"
    end: str # ex "2020-09-01T01:00:00Z" 또는 문자열로 cast한 밀리초단위 Unixtimestampe ex "1627607610302"
    send_topic: str # send topic -> "iot-sensor-data-p3-r1-retention1h"
    train_ratio: float # 데이터셋 train 비율.
    n_estimators: int # 모델학습 파라미터
    feature_option_list: str # 피쳐컬럼들. !!!!문자열로, ","로 구분자.!!!
    label_option: str # 라벨컬럼.


    '''
    temperature,rainfall,windspeed,humidity,month,day,time
    power_consumption
    '''