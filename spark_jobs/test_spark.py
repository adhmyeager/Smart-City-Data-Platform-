"""
spark_jobs/test_spark.py
Simplest possible Spark Streaming job.
Reads from Kafka vehicle-telemetry and prints to console.
No S3, no schemas — just prove Spark talks to Kafka.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("SmartCity-Test") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "vehicle-telemetry") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Just decode the value from bytes to string
decoded = df.select(
    F.col("value").cast("string").alias("message"),
    F.col("timestamp").alias("kafka_time")
)

query = decoded.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("=== Test job running — watching for messages ===")
query.awaitTermination()