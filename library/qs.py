import pyspark.sql
from delta import *
import pandas as pd

DATABASE_PATH = "./test2/delta-table"
FORMAT = "delta"

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.range(0, 5)
data.write.format("delta").mode("overwrite").save("./tmp/delta-table")
data.show()

df = spark.read.format("delta").load("./tmp/delta-table")
df.createOrReplaceTempView("test1")

query = "SELECT * FROM test1"
spark.sql(query).show()