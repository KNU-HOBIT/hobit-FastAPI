from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.protobuf.functions import from_protobuf
from sqlalchemy.orm import Session

import config.config_loader as config_loader
from models import Chunk  


def make_chunk_in_elapsed( spark, db :Session, bucket, measurement, tag_key, tag_value,
                           start_time, end_time, total_messages=None,
                             topic="iot-sensor-data-p3-r1-retention1h"):
    
    # Define a function to create Unix timestamp in milliseconds
    def to_unix_timestamp_ms(col_name):
        return (col(col_name).cast("long") * 1000 +
                col(col_name).cast("timestamp").substr(21, 3).cast("long"))

    # Define a window specification to calculate the difference between consecutive timestamps
    window_spec = Window.orderBy("timestamp")  

    df = read_from_kafka(spark, start_time, end_time, topic)
    
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

    if total_messages:
        print("baseline count:", total_messages)
    # print("record count : ",df.count())

    # Store the chunk information in the database
    for row in chunk.collect():
        new_chunk = Chunk(
            bucket=bucket,
            measurement=measurement,
            tagKey=tag_key,
            tagValue=tag_value,
            startTs=row.start_timestamp_unix,
            endTs=row.end_timestamp_unix,
            chunkDuration=row.chunk_duration,
            count=row['count']
        )
        db.add(new_chunk)
    db.commit()

def read_from_kafka(spark, start_time, end_time ,topic):

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



def train_model(spark, df):
    # 데이터 전처리 및 피처 엔지니어링
    # 모델 학습 및 평가
    print("in train_model" + df)
    print("모델학습..!")
    pass

def stop_spark_session(spark):
    """
    Spark 세션을 종료하고 세션에 관련된 정보를 로그로 기록합니다.
    
    Parameters:
        spark (SparkSession): 종료할 Spark 세션 객체
    """
    session_info = f"Stopping Spark session with App Name: {spark.sparkContext.appName}, App ID: {spark.sparkContext.applicationId}"
    spark.stop()
    print(session_info)
    pass





'''
schema = StructType(
        [
            StructField("index", IntegerType()),
            StructField("blk_no", StringType()),
            StructField("press3", IntegerType()),
            StructField("calc_press2", DoubleType()),
            StructField("press4", IntegerType()),
            StructField("calc_press1", DoubleType()),
            StructField("calc_press4", DoubleType()),
            StructField("calc_press3", DoubleType()),
            StructField("bf_gps_lon", DoubleType()),
            StructField("gps_lat", DoubleType()),
            StructField("speed", DoubleType()),
            StructField("in_dt", StringType()),
            StructField("move_time", DoubleType()),
            StructField("dvc_id", StringType()),
            StructField("dsme_lat", DoubleType()),
            StructField("press1", IntegerType()),
            StructField("press2", IntegerType()),
            StructField("work_status", IntegerType()),
            StructField("timestamp", StringType()),
            StructField("is_adjust", StringType()),
            StructField("move_distance", IntegerType()),
            StructField("weight", DoubleType()),
            StructField("dsme_lon", DoubleType()),
            StructField("in_user", StringType()),
            StructField("eqp_id", IntegerType()),
            StructField("blk_get_seq_id", IntegerType()),
            StructField("lot_no", StringType()),
            StructField("proj_no", StringType()),
            StructField("gps_lon", DoubleType()),
            StructField("seq_id", LongType()),
            StructField("bf_gps_lat", DoubleType()),
            StructField("blk_dvc_id", StringType()),
        ]
    )

schema = StructType(
        [
            StructField("index", IntegerType()),
            StructField("blk_no", StringType()),
            StructField("press3", IntegerType()),
            StructField("calc_press2", DoubleType()),
            StructField("press4", IntegerType()),
            StructField("calc_press1", DoubleType()),
            StructField("calc_press4", DoubleType()),
            StructField("calc_press3", DoubleType()),
            StructField("bf_gps_lon", DoubleType()),
            StructField("gps_lat", DoubleType()),
            StructField("speed", DoubleType()),
            StructField("in_dt", StringType()),
            StructField("move_time", DoubleType()),
            StructField("dvc_id", StringType()),
            StructField("dsme_lat", DoubleType()),
            StructField("press1", IntegerType()),
            StructField("press2", IntegerType()),
            StructField("work_status", IntegerType()),
            StructField("timestamp", StringType()),
            StructField("is_adjust", StringType()),
            StructField("move_distance", IntegerType()),
            StructField("weight", DoubleType()),
            StructField("dsme_lon", DoubleType()),
            StructField("in_user", StringType()),
            StructField("eqp_id", IntegerType()),
            StructField("blk_get_seq_id", IntegerType()),
            StructField("lot_no", StringType()),
            StructField("proj_no", StringType()),
            StructField("gps_lon", DoubleType()),
            StructField("seq_id", LongType()),
            StructField("bf_gps_lat", DoubleType()),
            StructField("blk_dvc_id", StringType()),
        ]
    )

    # .config("spark.executor.instances", spark_config['executor_instances'])
    # .config("spark.executor.cores", spark_config['executor_cores'])
    # .config("spark.executor.memory", spark_config['executor_memory'])

print("="*100)
# 모든 설정 값 출력
for key, value in spark_config.items():
    print(f"Config key: {key}, Value: {value}")
print("="*100)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", spark_config['kafka_bootstrap_servers'])
    .option("subscribe", "transport-932")
    .option("startingOffsets", "latest")
    .load()
) 
# Select and transform necessary columns
df = (df
      .selectExpr("CAST(value AS STRING) as value", "CAST(timestamp AS STRING) as createTime")
      .withColumn("createTime", F.to_timestamp("createTime", "yyyy-MM-dd HH:mm:ss.SSS"))
      .withColumn("value", F.from_json("value", schema)))

# Add fields from the schema as separate columns
for field in schema.fields:
    df = df.withColumn(field.name, df["value." + field.name])
F.array_distinct
# Select desired columns
select_columns = ["createTime", "timestamp", "eqp_id", "calc_press1", "calc_press2", "calc_press3", 
                  "calc_press4", "weight", "speed", "bf_gps_lat", "bf_gps_lon", "gps_lat", 
                  "gps_lon", "is_adjust", "dsme_lat", "dsme_lon", "lot_no"]

## createTime -> kafka메세지 생성 시간. (현재. 2024년 언저리 )
## timestamp -> 실제 데이터셋에 적혀있는 시간 ( 2020년 언저리 )
## eqp_id -> 트랜스포터 고유 ID ( PK )
## calc_press1,2,3,4 -> 트랜스포터의 4가지 press 센서 값.
## weight -> 위 4가지 센서값을 계산한 최종 무게값. ( 이것만 써도 무방. )
## speed -> 트랜스포터 속도
## bf_gps_lat, bf_gps_lon, gps_lat, gps_lon, is_adjust -> bf붙은건, 조정된 GPS 데이터. 아닌건 RAW GPS 데이터, 조정 여부는 is_adjust "Y" or "N"
## dsme_lat, dsme_lon -> ArcGis 좌표 데이터.
## lot_no : 트랜스포터 주 영역의 주소값

## 어떻게 지지고 볶을 것이냐? -> 윈도우당, 1) weight,  1) speed  최소최대평균  3) record 갯수.  4)...???

df = df.drop("value").select(*select_columns)

# Sliding window and single aggregation step
agg_df = (df.withWatermark("createTime", "0 seconds")
          .groupBy(F.window(F.col("createTime"), "1 minutes", "10 seconds"))
          .agg(F.count("*").alias("record_count"),
               F.min("createTime").alias("min_createTime"),
               F.max("createTime").alias("max_createTime"),
               F.max("weight").alias("max_weight"),
               F.min("weight").alias("min_weight"),
               F.avg("weight").alias("avg_weight"),
               F.max("speed").alias("max_speed"),
               F.min("speed").alias("min_speed"),
               F.avg("speed").alias("avg_speed"),
            ))

# Labeling driving/not driving
agg_df = agg_df.withColumn("is_driving", F.when(F.col("record_count") >= 8, F.lit("Driving")).otherwise(F.lit("Not Driving")))

# Labeling load status
agg_df = agg_df.withColumn("weight_variation", F.col("max_weight") - F.col("min_weight"))
agg_df = agg_df.withColumn("load_status", F.when((F.col("weight_variation") > 100) | (F.col("ls") > 100), F.lit("Loading")).otherwise(F.lit("Not Loading")))


agg_df = agg_df.withColumn("min_createTime", F.from_utc_timestamp(F.col("min_createTime"), "Asia/Seoul")) \
               .withColumn("max_createTime", F.from_utc_timestamp(F.col("max_createTime"), "Asia/Seoul")) \
               .withColumn("window_start", F.from_utc_timestamp(F.col("window.start"), "Asia/Seoul")) \
               .withColumn("window_end", F.from_utc_timestamp(F.col("window.end"), "Asia/Seoul")) \
               .drop("window")

def write_to_console_if_not_empty(batch_df: DataFrame, batch_id: int):
    if not batch_df.rdd.isEmpty():
        batch_df.show(truncate=False)

# Select final required columns
final_columns = ["window_start", "window_end", "min_createTime", "max_createTime", "record_count", "load_status", "is_driving", 
                 "max_weight","min_weight", "avg_weight", "max_speed", "min_speed", "avg_speed"]
# 좌표값을 반올림
agg_df = agg_df.withColumn("max_weight", F.round("max_weight", 5))
agg_df = agg_df.withColumn("min_weight", F.round("min_weight", 5))
agg_df = agg_df.withColumn("avg_weight", F.round("avg_weight", 5))
agg_df = agg_df.withColumn("max_speed", F.round("max_speed", 5))
agg_df = agg_df.withColumn("min_speed", F.round("min_speed", 5))
agg_df = agg_df.withColumn("avg_speed", F.round("avg_speed", 5))
agg_df = agg_df.select(*final_columns)



# Stream the results to console
query = (agg_df.select(F.to_json(F.struct(*final_columns)).alias("value"))
         .writeStream
         .outputMode("append")
         .foreachBatch(write_to_console_if_not_empty)
         .start())

query.awaitTermination()


# # 슬라이딩 윈도우 설정 및 윈도우 내 데이터 수집
# agg_df = df.withWatermark("createTime", "0 seconds") \
#     .groupBy(window(col("createTime"), "10 seconds", "10 seconds"))

## 원하는게 뭐야? 아래는 전부 윈도우 단위로 계산해야함.groupBy(window(col("createTime"), "10 seconds", "10 seconds")) 
## 실시간 운행정보 (최근 데이터가 10초동안 8개 이상 데이터가 들어왔는지.. ) ->  ( 운행 중 / 운행 X )  데이터 라벨링.
## 아래는 '운행 중'으로 라벨링되었을 경우에만. 아닐 경우는 null.
## 먼저 윈도우에 대한 정보 컬럼이 select되어야함. -> timestamp의 min, max.
## 적재상태. -> (최근 10초동안 weight의 변동폭이 엄청 크다! -> 물체 적재중으로 정의. / weight의 평균이 기준치(100)보다 높다.! -> 물체 적재중. / 낮다 -> 물체 적재X ) 데이터 라벨링.
## 최근 10초간 speed의 최대, 최소, 평균값 (agg 계산.)
## 최근 10초간 이동 거리. ->  각 record의 "bf_gps_lat", "bf_gps_lon"값을들 모두 이용해서, 각 record 사이사이마다 거리를 계산해서 마지막에 전부다 sum (좀 복잡한 agg 계산.)


# 'start'와 'end' 컬럼을 KST로 변환하고, 원본 'window' 컬럼 제거
agg_df = agg_df.withColumn("start", F.from_utc_timestamp(F.col("start"), "Asia/Seoul")) \
               .withColumn("end", F.from_utc_timestamp(F.col("end"), "Asia/Seoul"))
# agg_df.printSchema()
query = agg_df.select(
    F.to_json(F.struct(*final_columns)).alias("value")
)

# df.printSchema()

# example_df = df.select(max("speed")) # 최대값.
# example_df.printSchema()
# .agg(collect_list(struct(*[col(f) for f in select_columns])).alias("data_in_window"))
# # 집계 작업: 슬라이딩 윈도우 설정
# agg_df = df \
#     .withWatermark("createTime", "0 seconds") \
#     .groupBy(window(col("createTime"), "10 seconds", "10 seconds")) \
#     .count()
# # 'start'와 'end' 컬럼을 KST로 변환
# agg_df = agg_df.withColumn("start", from_utc_timestamp(col("window.start"), "Asia/Seoul")) \
#                .withColumn("end", from_utc_timestamp(col("window.end"), "Asia/Seoul")) \
#                .drop("window")

# # 결과를 Kafka로 스트리밍 전송
# kafka_query  = query.writeStream.format("kafka") \
#     .option("kafka.bootstrap.servers", spark_config['kafka_bootstrap_servers']) \
#     .option("topic", "my-topic") \
#     .option("checkpointLocation", "checkpoint") \
#     .outputMode("append") \
#     .start()

# 동일한 데이터를 콘솔에 출력하는 스트림 설정
# 콘솔에 데이터를 출력하는 스트림 설정
console_query = query.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_console_if_not_empty) \
    .start()

# kafka_query .awaitTermination()
console_query.awaitTermination()
'''
# 나중에, 모델 학습 코드 포팅할 때 필요한 코드들.
'''
from pyspark.sql.functions import col, abs, mean, expr, substring, udf
from pyspark.ml.feature import StringIndexer


def train_test_split(df, th):
    df = df.na.drop(subset=['일시'])
    train = df.filter(col('일시').substr(1, 8).cast(IntegerType()) < th)
    test = df.filter(col('일시').substr(1, 8).cast(IntegerType()) >= th)
    return train, test

def preprocess_x(df):
    to_remove_columns = ['num_date_time', '일시', '일조(hr)', '일사(MJ/m2)']
    df = df.fillna(0)   
    
    # 시계열 특성을 학습에 반영하기 위해 일시를 월, 일, 시간으로 나눕니다
    df = df.withColumn('month', substring('일시', 5, 2).cast(IntegerType()))
    df = df.withColumn('day', substring('일시', 7, 2).cast(IntegerType()))
    df = df.withColumn('time', substring('일시', 10, 2).cast(IntegerType()))
    
    
    
    df = df.join(building_sdf.select('건물번호', '건물유형', '연면적(m2)'), on='건물번호', how='left')
    df = df.dropDuplicates()
    
    
    # '건물유형'을 카테고리형 코드로 변환
    # building_type_indexer = StringIndexer(inputCol='건물유형', outputCol='건물유형_index')
    # df = building_type_indexer.fit(df).transform(df)
    # df = df.drop('건물유형').withColumnRenamed('건물유형_index', '건물유형')
    
    # 불필요한 컬럼 삭제
    for c in to_remove_columns:
        if c in df.columns:
            df = df.drop(c)
            
    df.show(20, truncate=False)
    return df

date_th = 20220820

train_df, valid_df = train_test_split(train_sdf, date_th)


#print("train_df DataFrame show:", train_df.show())
#print("valid_df DataFrame show:", valid_df.show())

# 데이터 분할 후 각 데이터프레임의 크기를 확인합니다.
print("train_df shape(split 후):", train_df.count(), len(train_df.columns))
print("valid_df shape(split 후):", valid_df.count(), len(valid_df.columns))

train_1 = preprocess_x(train_df)
#train_y = train_df.select("전력소비량(kWh)")

# 전처리 후 데이터프레임의 크기를 확인합니다.
print("train_1 shape:", train_1.count(), len(train_1.columns))
#print("Train Y shape:", train_y.count(), len(train_y.columns))

valid_1 = preprocess_x(valid_df)
#valid_y = valid_df.select("전력소비량(kWh)")

print("valid_1 shape:", valid_1.count(), len(valid_1.columns))
#print("Validation Y shape:", valid_y.count(), len(valid_y.columns))

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from tqdm import tqdm

def validate_multi(valid_1, models):
    """
    Args:
        models: dict, {1: model1, 2: model2, ..., 100: model100}
    """
    
    mse_eval = RegressionEvaluator(labelCol='전력소비량(kWh)', predictionCol='prediction', metricName='mse')
    rmse_eval = RegressionEvaluator(labelCol='전력소비량(kWh)', predictionCol='prediction', metricName='rmse')
    r2_eval = RegressionEvaluator(labelCol='전력소비량(kWh)', predictionCol='prediction', metricName='r2')

    predictions=[0 for _ in range(101)]
    for i in tqdm(range(1, 101)):
        aB = valid_1.filter(col('건물번호') == i)
        
        aB = aB.drop('건물번호', '건물유형', '연면적(m2)', '냉방면적(m2)', 'createTime')
        
        feature_cols = [c for c in aB.columns if c != '전력소비량(kWh)']
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        aB = assembler.transform(aB).select("features", "전력소비량(kWh)")
        
        
        predictions[i] = models[i].transform(aB)
        print('mse:', mse_eval.evaluate(predictions[i]), 'rmse:', rmse_eval.evaluate(predictions[i]), 'r2:', r2_eval.evaluate(predictions[i]))
        
    return predictions

from pyspark.ml.regression import RandomForestRegressor

def train_multiple_models(train_1, n_estimators=100):
    models = {}
    
    for i in tqdm(range(1, 101)):
        aBuilding = train_1.filter(col('건물번호') == i)
        
        aBuilding = aBuilding.drop('건물번호', '건물유형', '연면적(m2)', '냉방면적(m2)', 'createTime')
        
        #feature 벡터화
        feature_cols = [c for c in aBuilding.columns if c != '전력소비량(kWh)']
        assembler = VectorAssembler(inputCols = feature_cols, outputCol = "features")
        aBuilding = assembler.transform(aBuilding).select("features", "전력소비량(kWh)")
        
        rf = RandomForestRegressor(featuresCol='features', labelCol='전력소비량(kWh)', numTrees=n_estimators)
        aBuilding = aBuilding.repartition(200)
        model = rf.fit(aBuilding)
        
        models[i] = model
        
        
    return models

models1 = train_multiple_models(train_1)

predictions = []
predictions = validate_multi(valid_1, models1)


predictions[5].show()


predictions[5].show(predictions[5].count(), truncate=False)
'''