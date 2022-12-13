import pyspark.sql
import pyspark.sql.streaming
from delta import configure_spark_with_delta_pip
import time
from env import *
import csv


builder = pyspark.sql.SparkSession.builder \
    .master('local[4]') \
    .appName("ReadBatch") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

pre_time = time.time()
pre_count = 0

f = open('./2-speed-disk.csv', 'w')
writer = csv.writer(f)

while True:
    df = spark.read.format('delta').load("./data/data-1")
    
    delta_count = df.count() - pre_count
    pre_count = df.count()
    
    time_stamp = time.time() - pre_time
    pre_time = time.time()
    
    p = int(delta_count/time_stamp)
    print("P = {} r/s | D = {} | T = {}".format(p, delta_count, time_stamp))
    writer.writerow([p, delta_count, time_stamp])
    time.sleep(5)