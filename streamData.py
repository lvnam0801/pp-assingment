import pyspark.sql
import pyspark.sql.streaming
from delta import configure_spark_with_delta_pip
from env import *


builder = pyspark.sql.SparkSession.builder \
    .master('local[3]') \
    .appName("Stream") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", KAFKA_TOPIC) \
    .load() 

df.writeStream.format("delta") \
    .outputMode('append') \
    .option("path", "./data/data-1") \
    .option("checkpointLocation", "./data/cp-1") \
    .start() \
    .awaitTermination()