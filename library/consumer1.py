from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    app_name = "PySpark Delta Lake Streaming Example"
    master = "local"

    # Create Spark session with Delta extension

    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master(master)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    kafka_topic = "my-topic"
    kafka_servers = "localhost:9092"

    # Create a streaming DataFrame from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .writeStream.format("csv") \
        .option("checkpointLocation", "/data/delta/checkpoint") \
        .option("path", './b') \
        .start() \
        .awaitTermination()

    # # Write into delta table (/data/delta/kafka-events)
    # stream = df.selectExpr("CAST(key AS STRING) as key",
    #                        "CAST(value AS STRING) as value") \
        

    # time.sleep(120)

    # # Stop the stream
    # while stream.isActive:
    #     msg = stream.status['message']
    #     data_avail = stream.status['isDataAvailable']
    #     trigger_active = stream.status['isTriggerActive']
    #     if not data_avail and not trigger_active and msg != "Initializing sources":
    #         print('Stopping query...')
    #         stream.stop()
    #     time.sleep(0.5)

    # # Okay wait for the stop to happen
    # print('Awaiting termination...')
    # stream.awaitTermination()