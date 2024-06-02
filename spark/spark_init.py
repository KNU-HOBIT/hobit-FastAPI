import multiprocessing
import threading
import signal
import spark.chunk as chunk
import spark.ml as ml

def init_spark_process(target_func):
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    spark_process = multiprocessing.Process(target=target_func, args=(task_queue, result_queue))
    spark_process.start()

    return spark_process, task_queue, result_queue

class Spark_Session():

    def __init__(self, spark):
        '''
        생성 시, task_queue와 result_queue를 args로 갖는 함수를 요구.
        '''
        self.spark_process = None
        self.task_queue = None
        self.result_queue = None
        self.spark_process, self.task_queue, self.result_queue = init_spark_process(spark)
        # Removed join() to avoid blocking

    def get_spark(self):
        '''
        [0] = task_queue
        
        [1] = result_queue
        '''
        return self.task_queue, self.result_queue

def initialize_chunk_spark():
    global chunk_spark
    chunk_spark = Spark_Session(chunk.chunk_spark)

def initialize_ml_spark():
    global ml_spark
    ml_spark = Spark_Session(ml.ml_spark)

def signal_handler(sig, frame):
    print("Gracefully shutting down...")
    if 'chunk_spark' in globals():
        chunk_spark.spark_process.terminate()
        chunk_spark.spark_process.join()
    if 'ml_spark' in globals():
        ml_spark.spark_process.terminate()
        ml_spark.spark_process.join()
    thread_chunk.join()
    thread_ml.join()
    print("Shutdown complete.")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Initialize both Spark sessions in separate threads
thread_chunk = threading.Thread(target=initialize_chunk_spark)
thread_ml = threading.Thread(target=initialize_ml_spark)

thread_chunk.start()
thread_ml.start()

# Optionally, you can join the threads if you need to wait for them to finish
thread_chunk.join()
thread_ml.join()

# Now chunk_spark and ml_spark are available for use