from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

import config.config_loader as config_loader


spark_config = config_loader.get_config()['spark_config']
repartition_num = spark_config["executor_instances"] * spark_config["executor_cores"] * 2


spark = (
    SparkSession.builder.master(spark_config['master_url'])
    .appName(spark_config['app_name'])
    .config("spark.driver.bindAddress", spark_config['driver_bindAddress'])
    .config("spark.driver.host", spark_config['driver_host'])
    .config("spark.cores.max", "48")
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    .config("spark.dynamicAllocation.initialExecutors", spark_config['executor_instances'])
    .config("spark.dynamicAllocation.minExecutors", spark_config['executor_instances'])
    .config("spark.executor.memory", spark_config['executor_memory'])
    .config("spark.default.parallelism", repartition_num)
    .config("spark.sql.shuffle.partitions", repartition_num)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

def print_spark_session_configuration(spark):
    print("Current Spark configuration:")
    for key, value in sorted(spark.sparkContext._conf.getAll(), key=lambda x: x[0]):
        print(f"{key} = {value}")
    pass
print_spark_session_configuration(spark)

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


def run_spark_kafka_job(start_time, end_time, total_messages, topic):

    df = read_from_kafka(spark, start_time, end_time, topic)
    df.printSchema()
    df.show(n=1, truncate=False)

    print("baseline count:", total_messages)
    df = df.repartition(repartition_num)
    print("record count : ",df.count())
    
    
    return "모델 훈련 및 평가 완료"

def read_from_kafka(spark, start_time, end_time ,topic):

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", spark_config['kafka_bootstrap_servers'])
        .option("subscribe", topic)
        .option("startingOffsetsByTimestampStrategy", "latest")
        .option("startingTimestamp", str(start_time))
        .option("endingTimestamp", str(end_time))
        .load()
    ) 
    
    df = (
        df
        .selectExpr("CAST(value AS STRING) as value", "CAST(timestamp AS STRING) as createTime")
        # .withColumn("value", F.from_json("value", schema))
    )
    # for field in schema.fields:
    #     df = df.withColumn(field.name, df["value." + field.name])
    # df = df.drop("value")
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
