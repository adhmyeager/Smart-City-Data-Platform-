"""
spark_jobs/bronze_writer_local.py
Same as bronze_writer.py but writes to local filesystem instead of S3.
Use this to validate logic before connecting AWS.
"""

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, "/opt/spark_jobs")
from utils.schemas import (
    TELEMETRY_SCHEMA, WEATHER_SCHEMA,
    TRAFFIC_SCHEMA, ROAD_EVENT_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("bronze_writer_local")

KAFKA_BOOTSTRAP = "kafka:29092"

TOPICS = {
    "vehicle-telemetry": TELEMETRY_SCHEMA,
    "weather-data":      WEATHER_SCHEMA,
    "traffic-events":    TRAFFIC_SCHEMA,
    "road-events":       ROAD_EVENT_SCHEMA,
}

OUTPUT_BASE  = "/tmp/bronze"
TRIGGER_SECONDS = 30


def build_spark():
    spark = SparkSession.builder \
        .appName("SmartCity-BronzeLocal") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Bronze Writer LOCAL")
    return spark


def build_kafka_stream(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", f"spark-bronze-local-{topic}")
        .load()
        .withColumn("value",          F.col("value").cast(StringType()))
        .withColumn("kafka_topic",     F.col("topic"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_offset",    F.col("offset"))
        .withColumn("kafka_timestamp", F.col("timestamp"))
        .drop("key", "topic", "partition", "offset", "timestamp", "timestampType")
    )


def parse_json_stream(raw_df, schema):
    return (
        raw_df
        .withColumn("payload",        F.from_json(F.col("value"), schema))
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("partition_date", F.date_format(F.col("kafka_timestamp"), "yyyy-MM-dd"))
        .withColumn("partition_hour", F.hour(F.col("kafka_timestamp")))
        .select(
            "payload.*",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_time",
            "partition_date",
            "partition_hour",
        )
    )


def start_bronze_query(spark, topic, schema):
    # Local paths instead of S3
    local_dest = f"{OUTPUT_BASE}/{topic}"
    ckpt       = f"/tmp/checkpoints/bronze_{topic.replace('-', '_')}"

    log.info(f"[{topic}] Output → {local_dest}")
    log.info(f"[{topic}] Checkpoint → {ckpt}")

    raw_df    = build_kafka_stream(spark, topic)
    parsed_df = parse_json_stream(raw_df, schema)

    query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path",               local_dest)
        .option("checkpointLocation", ckpt)
        .partitionBy("partition_date", "partition_hour")
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(f"bronze_{topic.replace('-', '_')}")
        .start()
    )

    log.info(f"[{topic}] Query started — id={query.id}")
    return query


def main():
    log.info("=" * 60)
    log.info("  Smart City — Bronze Writer (LOCAL MODE)")
    log.info(f"  Output base : {OUTPUT_BASE}")
    log.info(f"  Trigger     : {TRIGGER_SECONDS}s")
    log.info("=" * 60)

    spark   = build_spark()
    queries = []

    for topic, schema in TOPICS.items():
        try:
            q = start_bronze_query(spark, topic, schema)
            queries.append(q)
        except Exception as e:
            log.error(f"[{topic}] Failed: {e}", exc_info=True)
            raise

    log.info(f"All {len(queries)} bronze queries running...")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.warning("Stopping all queries...")
        for q in queries:
            q.stop()

    log.info("Bronze Writer LOCAL stopped.")


if __name__ == "__main__":
    main()