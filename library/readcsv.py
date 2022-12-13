import pyspark.sql
import pyspark.sql.streaming
from delta import configure_spark_with_delta_pip


KAFKA_HOST = "localhost:9092"
KAFKA_TOPIC = "my-topic"

builder = pyspark.sql.SparkSession.builder.appName("ReadCSV")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.csv('./dt')
df.toDF().show()