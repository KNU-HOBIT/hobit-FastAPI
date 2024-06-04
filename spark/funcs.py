
import config.config_loader as config_loader
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

from request.request_crud import data_by_time_range_req


from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer

def read_from_kafka_json(spark, start_time, end_time, topic):

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config_loader.get_config()['spark_config']['kafka_bootstrap_servers'])
        .option("subscribe", topic)
        .option("startingOffsetsByTimestampStrategy", "latest")
        .option("startingTimestamp", str(start_time))
        .option("endingTimestamp", str(end_time))
        .load()
    ) 

    df = (
        df
        .selectExpr("CAST(value AS STRING) as value", "CAST(timestamp AS STRING) as createTime")
        .withColumn("value", unbase64("value"))
        .withColumn("decoded_value", from_protobuf("value", "Transport", descFilePath="proto/hobit.desc"))
        .select("createTime", "decoded_value.*")
    )

    return df

def read_from_kafka_protobuf(spark, start_time, end_time ,topic, proto_message_type):

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", config_loader.get_config()['spark_config']['kafka_bootstrap_servers'])
        .option("subscribe", topic)
        .option("startingOffsetsByTimestampStrategy", "latest")
        .option("startingTimestamp", str(start_time))
        .option("endingTimestamp", str(end_time))
        .load()
    ) 
    
    df = (
        df
        .selectExpr("value", "CAST(timestamp AS STRING) as createTime")
        .withColumn("decoded_value", from_protobuf("value", proto_message_type, descFilePath="proto/hobit.desc"))
        .select("createTime", "decoded_value.*")
    )

    return df


def chunk_job(df):
    '''
    args
    df : timestamp 컬럼을 포함한 dataframe
    '''
    # Define a function to create Unix timestamp in milliseconds
    def to_unix_timestamp_ms(col_name):
        return (col(col_name).cast("long") * 1000 +
                col(col_name).cast("timestamp").substr(21, 3).cast("long"))
    window_spec = Window.orderBy("timestamp")  

    df = (df
        # Convert the timestamp column to a timestamp type
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        # Sort the DataFrame by timestamp
        .orderBy("timestamp")
        .withColumn("prev_timestamp", lag("timestamp", 1).over(window_spec)) 
    )

    df.na.fill(0) # NULL -> Zero on prev_timestamp

    df = (df
        # Calculate the difference between consecutive timestamps in milliseconds
        .withColumn("time_diff", to_unix_timestamp_ms("timestamp") - 
                                to_unix_timestamp_ms("prev_timestamp"))
        # Identify the start of each new chunk (greater than 10 min)                   
        .withColumn("is_new_chunk", when((col("time_diff") > 600000), 1).otherwise(0))
        # Use cumulative sum to assign chunk IDs
        .withColumn("chunk_id", sum("is_new_chunk").over(window_spec)) 
    )

    # Calculate the duration of each chunk by finding the max and min timestamp per chunk and their difference in milliseconds
    chunk = (
        df.groupBy("chunk_id")
        .agg(
            min("timestamp").alias("start_timestamp"),
            max("timestamp").alias("end_timestamp"),
            count("*").alias("count")
        )
        .filter(col("count") >= 2)  # Filter out groups with count less than 5
        .withColumn("start_timestamp_ms", to_unix_timestamp_ms("start_timestamp"))
        .withColumn("end_timestamp_ms", to_unix_timestamp_ms("end_timestamp"))
        .withColumn("chunk_duration", col("end_timestamp_ms") - col("start_timestamp_ms"))
        .withColumn("start_timestamp_unix", col("start_timestamp_ms"))
        .withColumn("end_timestamp_unix", col("end_timestamp_ms"))
        .drop("start_timestamp_ms", "end_timestamp_ms")
    )

    # chunk_durations.printSchema()
    chunk.show(truncate=False)
    # +--------+-----------------------+-----------------------+-----+--------------+--------------------+------------------+
    # |chunk_id|start_timestamp        |end_timestamp          |count|chunk_duration|start_timestamp_unix|end_timestamp_unix|
    # +--------+-----------------------+-----------------------+-----+--------------+--------------------+------------------+
    # |0       |2020-09-01 01:07:35.165|2020-09-01 02:33:14.03 |4837 |5138838       |1598890055165       |1598895194003     |
    # |1       |2020-09-01 04:43:55.001|2020-09-01 05:37:43.11 |3218 |3228010       |1598903035001       |1598906263011     |
    # |2       |2020-09-01 08:39:50.622|2020-09-01 10:28:28.214|6459 |6517592       |1598917190622       |1598923708214     |
    # +--------+-----------------------+-----------------------+-----+--------------+--------------------+------------------+

    # Sum up all chunk durations to get the cumulative run time
    
    # total_cumulative_run_time = chunk.agg(sum("chunk_duration").alias("total_run_time")).collect()[0]["total_run_time"]
    # print("total_cumulative_run_time", total_cumulative_run_time)


    # Store the chunk information in the database
    return chunk.collect()


def train_model(
    df,
    train_ratio, 
    n_estimators, 
    feature_option_list, 
    label_option
):
    '''
    데이터 전처리 및 피처 엔지니어링
    모델 학습 및 평가

    feature_option_list = 선택한 feature컬럼 list

    label_option= 선택한 label 컬럼명 (string)
    
    '''
    
    df.show(n=1, truncate=False)
    df.printSchema()

    columns_to_retain = [label_option] + feature_option_list
    df = df.select([c for c in df.columns if c in columns_to_retain])
    
    for feature in feature_option_list:
        if dict(df.dtypes)[feature] == 'string':
            indexer = StringIndexer(inputCol=feature, outputCol=f"{feature}_index")
            df = indexer.fit(df).transform(df)
            df = df.drop(feature).withColumnRenamed(f"{feature}_index", feature)
            
    train_df, valid_df = split_spark_dataframe(df, train_ratio)  
    
    #feature 벡터화
    feature_cols_t = feature_option_list
    assembler = VectorAssembler(inputCols = feature_cols_t, outputCol = "features")
    train_df = assembler.transform(train_df).select("features", label_option)
    
    rf = RandomForestRegressor(featuresCol='features', labelCol=label_option, numTrees=n_estimators)
    train_df = train_df.repartition(64)
    model = rf.fit(train_df)
    #모델학습 완료
    
    #validate 시작
    mse_eval = RegressionEvaluator(labelCol=label_option, predictionCol='prediction', metricName='mse')
    rmse_eval = RegressionEvaluator(labelCol=label_option, predictionCol='prediction', metricName='rmse')
    r2_eval = RegressionEvaluator(labelCol=label_option, predictionCol='prediction', metricName='r2')

    # [c for c in aB.columns if c != label_option]
    feature_cols_v = feature_option_list
    assembler_v = VectorAssembler(inputCols=feature_cols_v, outputCol="features")
    valid_df = assembler_v.transform(valid_df).select("features", label_option)
        
        
    prediction = model.transform(valid_df)
#    print('mse:', mse_eval.evaluate(prediction), 'rmse:', rmse_eval.evaluate(prediction), 'r2:', r2_eval.evaluate(predictions))
    
    #Actual vs prediction sdf
    finish_sdf = prediction.select(label_option, "prediction")
    
    finish_pdf = finish_sdf.toPandas()
    
    finish_pdf = finish_pdf.where((pd.notnull(finish_pdf)), None)

    result = {
        "mse" : mse_eval.evaluate(prediction),
        "rmse" : rmse_eval.evaluate(prediction),
        "r2" : r2_eval.evaluate(prediction),
        "powerConsumptionList" : finish_pdf[label_option].tolist(),
        "predictionList" : finish_pdf['prediction'].tolist(),
    }

    return result

def split_spark_dataframe(df, train_ratio, seed=None):
    # train_ratio를 기반으로 valid_ratio 계산
    valid_ratio = 1 - train_ratio
    
    # 비율 설정
    weights = [train_ratio, valid_ratio]
    
    # 데이터프레임 분할
    train_df, valid_df = df.randomSplit(weights, seed)
    
    return train_df, valid_df
