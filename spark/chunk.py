from pyspark.sql import SparkSession
import config.config_loader as config_loader
import spark.funcs as sf
import spark.util as util

def chunk_spark(task_queue, result_queue):
    spark_config = config_loader.get_config()['spark_config']
    repartition_num = 16 * 2
    jar_urls = ",".join(spark_config["kafka_jars"])

    chunk_spark = (
        SparkSession.builder
        .appName("chunk_spark_APP")
        .master(spark_config['master_url'])
        .config("spark.driver.bindAddress", spark_config['driver_bindAddress'])
        .config("spark.driver.host", spark_config['driver_host'])
        .config("spark.cores.max", "16")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.dynamicAllocation.initialExecutors", spark_config['executor_instances'])
        .config("spark.dynamicAllocation.minExecutors", spark_config['executor_instances'])
        .config("spark.executor.memory", spark_config['executor_memory'])
        .config("spark.default.parallelism", repartition_num)
        .config("spark.sql.shuffle.partitions", repartition_num)
        .config("spark.jars", jar_urls)
        .getOrCreate()
    )

    chunk_spark.sparkContext.setLogLevel('ERROR')

    print("="*100)
    util.print_spark_session_configuration(chunk_spark)
    print("="*100)  

    while True:
        task, args = task_queue.get()
        if task == 'STOP':
            break
        if task == 'PROCESS_CHUNK':
            result_queue.put(
                sf.chunk_job(
                    sf.read_from_kafka_protobuf(
                        chunk_spark, 
                        args.get("start_time"),
                        args.get("end_time"),
                        args.get("topic"),
                        args.get("proto_message_type")
                    )
                )
            )
    chunk_spark.stop()