import pyspark.sql
from delta import *
# data structure Data Warehouse: DataFrame :row - column
# data structure store in DataLake: csv, delta, txt

DATABASE_PATH = "./tmp/delta-table"
FORMAT = "delta"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .master('local[1]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# create table
# columns = ['id', 'dogs', 'cats']
# vals = [(1, 2, 0), (2, 0, 1), (1, 2, 3), (0, 0, 0)]
# data = spark.createDataFrame(vals, columns)


# create table
# data.write.format(FORMAT).mode("append").save(DATABASE_PATH)

# read data
# df = spark.read.format(FORMAT).load(DATABASE_PATH)
# df.show()

# read old version
df = spark.read.format("delta").option("versionAsOf", 0).load("./tmp/delta-table")
df.show()

# streamingDf = spark.readStream.format("rate").load()
# stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "./tmp/checkpoint").start("./tmp/delta-table")

# read stream from table
# stream2 = spark.readStream.format("rate").load().writeStream.format("console").start().awaitTermination()



# read
# write

### delta-lake
# data: update, insert, overwrite,...
# data version : rollback

# data Batch: 
# read -> flush -> send: BatchSIZE=50MB
# write -> 50MB
# 50MB/Batch -> 100MB/s
# 100MB/Batch -> 50MB/s
# 70MB/Batch -> 150MB/s

# streaming... -> data income...
# readStream : flush() -> batch : 50MB timestamp: 1 -> data1
# readStream : flush() -> batch : 50MB timestamp: 2 -> data2

# writeStream : data1
# writeStream: data2



