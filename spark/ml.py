from pyspark.sql import SparkSession
import config.config_loader as config_loader
import spark.funcs as sf
import spark.util as util

def ml_spark(task_queue, result_queue):
    spark_config = config_loader.get_config()['spark_config']
    repartition_num = 32 * 2
    jar_urls = ",".join(spark_config["kafka_jars"])

    ml_spark = (
        SparkSession.builder
        .appName("ml_spark_APP")
        .master(spark_config['master_url'])
        .config("spark.driver.bindAddress", spark_config['driver_bindAddress'])
        .config("spark.driver.host", spark_config['driver_host'])
        .config("spark.cores.max", "32")
        .config("spark.executor.instances", spark_config["executor_instances"])
        .config("spark.executor.cores", spark_config["executor_cores"])
        .config("spark.executor.memory", spark_config["executor_memory"])
        .config("spark.driver.memory", "30g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "20g")
        .config("spark.default.parallelism", repartition_num)
        .config("spark.sql.shuffle.partitions", repartition_num)
        .config("spark.jars", jar_urls)
        .getOrCreate()
    )

    ml_spark.sparkContext.setLogLevel('ERROR')

    print("="*100)  
    util.print_spark_session_configuration(ml_spark)
    print("="*100)  

    while True:
        task, args = task_queue.get()
        if task == 'STOP':
            break
        if task == 'PROCESS_ML':
            result_queue.put(
                sf.train_model(
                    sf.read_from_kafka_unbase64_from_protobuf(
                        ml_spark, 
                        args.get("start_time"),
                        args.get("end_time"),
                        args.get("topic"),
                        args.get("proto_message_type")
                    ), 
                    args.get("train_ratio"), 
                    args.get("n_estimators"), 
                    args.get("feature_option_list"),
                    args.get("label_option"),
                )
            )
    ml_spark.stop()
