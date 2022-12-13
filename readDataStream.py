import pyspark.sql
import pyspark.sql.streaming
from delta import configure_spark_with_delta_pip
from env import *

builder = pyspark.sql.SparkSession.builder.appName("DeltaApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def GetP(df):
    print(df.count)
    
df = spark.readStream \
    .format('delta') \
    .load(DATA_PATH)
# df.show()
df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
    .writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
