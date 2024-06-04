from sqlalchemy.orm import Session
from spark.spark_schema import MLReq
from spark.spark_init import ml_spark
import spark.util as util
from request.request_crud import data_by_time_range_req
from models import TrainResult
import time



def do_ml(req :MLReq, db:Session):

    responce_result = data_by_time_range_req(
        start=req.start, 
        end=req.end, 
        bucket=req.bucket, 
        measurement=req.measurement, 
        tag_key=req.tag_key, 
        tag_value=req.tag_value 
    )

    if req.measurement == "transport": proto_msg_type = "Transport"
    elif req.measurement == "electric_dataset": proto_msg_type = "Electric"
    else: 
        print("Can't found PROTOBUF MESSAGE TYPE")
        return
    
    splited_feature_option_list = req.feature_option_list.split(",")

    ml_job_arg = \
    { 
        "start_time" : responce_result.get("startTimeMillis", None),
        "end_time" : responce_result.get("endTimeMillis", None), 
        "topic" : responce_result.get("sendTopicStr", None), 
        "proto_message_type": proto_msg_type,
        "train_ratio" : req.train_ratio,
        "n_estimators" : req.n_estimators,
        "feature_option_list" : splited_feature_option_list,
        "label_option" : req.label_option,
    }

    start_time = int(time.time() * 1000)  # 현재 시간을 밀리초 단위로 기록

    result = util.process_spark_tasks(
        ml_spark.get_spark()[0],
        ml_spark.get_spark()[1], 
        'PROCESS_ML',
        ml_job_arg, 
    )

    end_time = int(time.time() * 1000)  # 작업 종료 시간을 밀리초 단위로 기록

    '''
    result = {
        "mse" : mse_eval.evaluate(prediction),
        "rmse" : rmse_eval.evaluate(prediction),
        "r2" : r2_eval.evaluate(prediction),
        "powerConsumptionList" : finish_pdf[label_option].tolist(),
        "predictionList" : finish_pdf['prediction'].tolist(),
    }
    '''
    # TrainResult 객체 생성 및 데이터베이스에 저장
    train_result = TrainResult(
        bucket=req.bucket,
        measurement=req.measurement,
        tagKey=req.tag_key,
        tagValue=req.tag_value,
        mlStart=start_time,
        mlEnd=end_time,
        dataStart=req.start,
        dataEnd=req.end,
        trainRatio=req.train_ratio,
        nEstimators=req.n_estimators,
        featureOptionList=splited_feature_option_list,
        label_option=req.label_option,
        mse=result.get('mse', None),
        rmse=result.get('rmse', None),
        r2=result.get('r2', None),
        powerConsumptionList=result.get('powerConsumptionList', []),
        predictionList=result.get('predictionList', [])
    )
    db.add(train_result)
    db.commit()


    return result