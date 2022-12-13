# start kafka : localhost:9092
`./bin/kafka-server-start.sh ./config/kraft/server.properties`
`./bin/kafka-server-start.sh ./config/kraft/server.properties`

# start producer
`python3 producer.py`

# start spark: read-write-streaming
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0`

# version of:
`pyspark==3.3.0`
`kafka==3.3.1`
`delta-core==2.2.0`

# format kafka system
`./bin/kafka-storage.sh random-uuid`
`./bin/kafka-storage.sh format -t <uuid> -c ./config/kraft/server.properties`

# start kafka with spark - delta
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pathFile`

# start hadoop - format : hdfs://localhost:9000
`./bin/hdfs namenode -format`
`sbin/start-dfs.sh`