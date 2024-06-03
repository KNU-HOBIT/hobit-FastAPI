
from sqlalchemy.orm import Session
from models import TrainResult
from dataset.dataset_schema import MLList, datasetSelection, datasetOption

def list_All_ML(db: Session):
    lists = db.query(TrainResult).filter().all()
    return [MLList(bucket=row.bucket, measurement=row.measurement, tagKey=row.tagKey, tagValue=row.tagValue, mlStart=row.mlStart, mlEnd=row.mlEnd) for row in lists]


def get_ML(db:Session , mlStart : int):
    ML = db.query(TrainResult).filter(TrainResult.mlStart==mlStart).first()
    return ML
