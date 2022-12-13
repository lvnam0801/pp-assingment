from delta.tables import *
from pyspark.sql.functions import *
import pyspark.sql
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

deltaTable = DeltaTable.forPath(spark, "./tmp/delta-table")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = expr("id % 2 == 0"),
  set = { "id": expr("id + 100") })

deltaTable.toDF().show()

# Delete every even value
deltaTable.delete(condition = expr("id % 2 == 0"))

# Upsert (merge) new data
newData = spark.range(0, 20)

deltaTable.alias("oldData") \
    .merge(newData.alias("newData"), "oldData.id = newData.id") \
    .whenMatchedUpdate(set = { "id": col("newData.id") }) \
    .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
    .execute()

deltaTable.toDF().show()

# data warehouse
# data lake
# delta lake: lake house: databrick : delta