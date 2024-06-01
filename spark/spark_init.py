from pyspark.sql import SparkSession

import config.config_loader as config_loader

spark = None

def print_spark_session_configuration(spark):
    print("Current Spark configuration:")
    for key, value in sorted(spark.sparkContext._conf.getAll(), key=lambda x: x[0]):
        print(f"{key} = {value}")

def load_spark_session():
    global spark
    spark_config = config_loader.get_config()['spark_config']
    repartition_num = 16 * 2
    jar_urls = ",".join(spark_config["kafka_jars"])
    spark = (
        SparkSession.builder.master(spark_config['master_url'])
        .appName(spark_config['app_name'])
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
    spark.sparkContext.setLogLevel('ERROR')
    
load_spark_session()
print_spark_session_configuration(spark)

def get_spark():
    return spark
