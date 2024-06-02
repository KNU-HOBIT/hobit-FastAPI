
def print_spark_session_configuration(spark):
    print("Current Spark configuration:")
    for key, value in sorted(spark.sparkContext._conf.getAll(), key=lambda x: x[0]):
        print(f"{key} = {value}")


def process_spark_tasks(task_queue, result_queue, task_type, task_args):
    task_queue.put((task_type, task_args))
    return result_queue.get()
    

