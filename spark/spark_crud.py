from sqlalchemy.orm import Session
from spark.spark_schema import MLReq
from spark.spark_init import ml_spark
import spark.util as util
from request.request_crud import data_by_time_range_req


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

    ml_job_arg = \
    { 
        "start_time" : responce_result.get("startTimeMillis", None),
        "end_time" : responce_result.get("endTimeMillis", None), 
        "topic" : responce_result.get("sendTopicStr", None), 
        "proto_message_type": proto_msg_type,
        "train_ratio" : req.train_ratio,
        "n_estimators" : req.n_estimators,
        "feature_option_list" : req.feature_option_list,
        "label_option" : req.label_option,
    }

    result = util.process_spark_tasks(
        ml_spark.get_spark()[0],
        ml_spark.get_spark()[1], 
        'PROCESS_ML',
        ml_job_arg, 
    )

    '''
    result = {
        "mse" : mse_eval.evaluate(prediction),
        "rmse" : rmse_eval.evaluate(prediction),
        "r2" : r2_eval.evaluate(prediction),
        "power_consumption_list" : finish_pdf[label_option].tolist(),
        "prediction_list" : finish_pdf['prediction'].tolist(),
    }
    '''


    return result