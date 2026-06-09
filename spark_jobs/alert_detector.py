"""
spark_jobs/alert_detector.py

Layer: Kafka vehicle-telemetry  →  Kafka alerts topic
       (real-time, sub-second latency path — bypasses S3 entirely)

Why a separate job?
  bronze_writer.py writes to S3 every 30s (micro-batch).
  Grafana/operators need alerts in seconds, not minutes.
  This job reads from Kafka directly and publishes anomalies
  back to the `alerts` Kafka topic immediately.

Alert rules (match simulator config.py):
  SPEED      speed_kmh      > 120.0
  ENGINE     engine_temp_c  > 105.0
  FUEL       fuel_level_pct < 10.0
  RPM        rpm            > 5000

Additional complex rules (not in simulator):
  RAPID_DECEL   acceleration_ms2 < -4.0  (hard braking / collision indicator)
  IDLE_OVERHEAT engine_temp_c > 98 AND speed_kmh < 5 (stuck in traffic, overheating)

Output to Kafka `alerts` topic — each row is a JSON alert with:
  alert_id, vehicle_id, alert_type, severity, timestamp,
  speed_kmh, engine_temp_c, fuel_pct, rpm, latitude, longitude

Run:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark_jobs/alert_detector.py
"""
"""
spark_jobs/alert_detector.py

Layer: Kafka vehicle-telemetry  →  Kafka alerts topic
       (real-time, sub-second latency — bypasses S3 entirely)

FIX LOG :
  - Checkpoint moved to /tmp/checkpoints (was s3a:// — no S3 JARs needed for Kafka→Kafka)
  - Alert rules rewritten as a single-pass column expression (no streaming union)
    Streaming union of N filtered DataFrames causes "multiple streaming aggregation"
    errors in Spark Structured Streaming. Use F.when chains instead.
  - submit command uses --jars (local files) not --packages (no internet in Docker)
  - Progress logging moved to foreachBatch to avoid blocking main thread

Run (use local JARs — no internet needed inside Docker):
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar,\
/opt/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/extra/kafka-clients-3.5.0.jar,\
/opt/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/extra/commons-pool2-2.11.1.jar \
      /opt/spark_jobs/alert_detector.py
"""

import os
import sys
import logging
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

sys.path.insert(0, "/opt/spark_jobs")
from utils.schemas import TELEMETRY_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger("alert_detector")

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_IN:        str = "vehicle-telemetry"
TOPIC_OUT:       str = "alerts"
TRIGGER_SECONDS: int = int(os.getenv("ALERT_TRIGGER_SECONDS", "5"))

# Alert thresholds — must match config.py
T_SPEED:       float = float(os.getenv("ALERT_SPEED_KMH",     "120.0"))
T_ENGINE_TEMP: float = float(os.getenv("ALERT_ENGINE_TEMP_C", "105.0"))
T_FUEL:        float = float(os.getenv("ALERT_FUEL_PCT",      "10.0"))
T_RPM:         int   = int(os.getenv("ALERT_RPM",             "5000"))
T_HARD_BRAKE:  float = -4.0
T_IDLE_TEMP:   float = 98.0

# Local checkpoint — no S3 needed for Kafka→Kafka job
CHECKPOINT_DIR: str = os.getenv("ALERT_CHECKPOINT_DIR", "/tmp/checkpoints/alert_detector")


# ─────────────────────────────────────────────────────────────
# SparkSession  (no S3 config needed — pure Kafka job)
# ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("SmartCity-AlertDetector")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.kafka.consumer.cache.enabled", "false")
        # Prevent Kafka consumer rebalance timeout on slow batches
        .config("spark.kafka.consumer.request.timeout.ms", "60000")
        .config("spark.kafka.consumer.session.timeout.ms", "30000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — Alert Detector (Kafka-only, no S3)")
    return spark


# ─────────────────────────────────────────────────────────────
# Alert rule engine  — SINGLE PASS (no streaming union)
# ─────────────────────────────────────────────────────────────
#
# Key design: instead of filtering N times and unioning N streams
# (which Spark rejects), we:
#   1. Tag each row with which rules it fires (multi-valued)
#   2. Explode that array → one row per fired rule
#   3. Build the Kafka payload in a single select
#
# This is a single streaming query — Spark is happy.

def tag_alert_rules(df: DataFrame) -> DataFrame:
    """
    Add an `alert_tags` array column — each element is a struct with
    (rule_name, alert_type, severity).  Rows that fire no rule get an
    empty array and are filtered out by the subsequent explode.
    """
    rules = F.array(
        # Each element: when condition fires → struct, else null
        F.when(
            F.col("speed_kmh") > T_SPEED,
            F.struct(
                F.lit("overspeed").alias("rule_name"),
                F.lit("SPEED_ALERT").alias("alert_type"),
                F.lit("HIGH").alias("severity"),
            )
        ),
        F.when(
            F.col("engine_temp_c") > T_ENGINE_TEMP,
            F.struct(
                F.lit("engine_overheat").alias("rule_name"),
                F.lit("ENGINE_TEMP_ALERT").alias("alert_type"),
                F.lit("CRITICAL").alias("severity"),
            )
        ),
        F.when(
            F.col("fuel_level_pct") < T_FUEL,
            F.struct(
                F.lit("low_fuel").alias("rule_name"),
                F.lit("FUEL_ALERT").alias("alert_type"),
                F.lit("MEDIUM").alias("severity"),
            )
        ),
        F.when(
            F.col("rpm") > T_RPM,
            F.struct(
                F.lit("high_rpm").alias("rule_name"),
                F.lit("RPM_ALERT").alias("alert_type"),
                F.lit("HIGH").alias("severity"),
            )
        ),
        F.when(
            F.col("acceleration_ms2") < T_HARD_BRAKE,
            F.struct(
                F.lit("hard_braking").alias("rule_name"),
                F.lit("HARD_BRAKE_ALERT").alias("alert_type"),
                F.lit("HIGH").alias("severity"),
            )
        ),
        F.when(
            (F.col("engine_temp_c") > T_IDLE_TEMP) & (F.col("speed_kmh") < 5),
            F.struct(
                F.lit("idle_overheat").alias("rule_name"),
                F.lit("IDLE_OVERHEAT_ALERT").alias("alert_type"),
                F.lit("HIGH").alias("severity"),
            )
        ),
    )

    return (
        df
        # Tag with all matching rules (nulls for non-matching)
        .withColumn("_rules_raw", rules)
        # Filter nulls from the array
        .withColumn(
            "alert_tags",
            F.array_compact(F.col("_rules_raw"))
        )
        # Only keep rows that fired at least one rule
        .filter(F.size(F.col("alert_tags")) > 0)
        .drop("_rules_raw")
    )


def build_alert_payload(df: DataFrame) -> DataFrame:
    """
    Explode alert_tags → one row per fired rule, then build Kafka payload.
    Result columns: key (vehicle_id bytes), value (JSON bytes).
    """
    exploded = (
        df
        .withColumn("rule", F.explode(F.col("alert_tags")))
        .withColumn("rule_name",  F.col("rule.rule_name"))
        .withColumn("alert_type", F.col("rule.alert_type"))
        .withColumn("severity",   F.col("rule.severity"))
        .drop("alert_tags", "rule")
    )

    # Deterministic alert_id (vehicle + unix time + rule — deduplicates on retry)
    exploded = exploded.withColumn(
        "alert_id",
        F.concat_ws("_", F.col("vehicle_id"), F.col("timestamp_unix").cast(StringType()), F.col("rule_name")),
    )

    alert_struct = F.struct(
        F.col("alert_id"),
        F.col("vehicle_id"),
        F.col("alert_type"),
        F.col("severity"),
        F.col("rule_name"),
        F.col("timestamp_iso").alias("timestamp"),
        F.col("timestamp_unix"),
        F.round(F.col("speed_kmh"),        2).alias("speed_kmh"),
        F.round(F.col("engine_temp_c"),    1).alias("engine_temp_c"),
        F.round(F.col("fuel_level_pct"),   2).alias("fuel_pct"),
        F.col("rpm"),
        F.round(F.col("acceleration_ms2"), 3).alias("acceleration_ms2"),
        F.round(F.col("latitude"),         6).alias("latitude"),
        F.round(F.col("longitude"),        6).alias("longitude"),
        F.col("road_type"),
        F.col("road_event"),
        F.col("vehicle_type"),
        F.col("route_name"),
        F.col("trip_id"),
    )

    return (
        exploded
        .withColumn("value", F.to_json(alert_struct))
        .withColumn("key",   F.col("vehicle_id"))
        .select("key", "value")
    )


# ─────────────────────────────────────────────────────────────
# Progress logging via foreachBatch (non-blocking)
# ─────────────────────────────────────────────────────────────

_batch_count = [0]  # mutable counter shared with foreachBatch closure

def _log_batch_progress(batch_df: DataFrame, batch_id: int) -> None:
    """Called once per micro-batch — logs alert count then discards the df."""
    count = batch_df.count()
    _batch_count[0] += 1
    log.info(f"[alert_detector] batch={batch_id} | alerts_fired={count} | total_batches={_batch_count[0]}")


# ─────────────────────────────────────────────────────────────
# Streaming pipeline
# ─────────────────────────────────────────────────────────────

def run(spark: SparkSession) -> None:
    log.info(f"Kafka source  : {KAFKA_BOOTSTRAP} topic={TOPIC_IN}")
    log.info(f"Kafka sink    : {TOPIC_OUT}")
    log.info(f"Trigger       : {TRIGGER_SECONDS}s")
    log.info(f"Checkpoint    : {CHECKPOINT_DIR}")

    # ── 1. Read raw telemetry from Kafka ─────────────────────
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 2000)
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "spark-alert-detector")
        .load()
        .withColumn("value", F.col("value").cast(StringType()))
    )

    # ── 2. Parse JSON against telemetry schema ────────────────
    parsed_df = (
        raw_df
        .withColumn("payload", F.from_json(F.col("value"), TELEMETRY_SCHEMA))
        .select("payload.*")
        .dropna(subset=["vehicle_id", "timestamp_unix", "speed_kmh"])
    )

    # ── 3. Tag + explode alert rules (single streaming query) ─
    tagged_df = tag_alert_rules(parsed_df)

    # ── 4. Build Kafka output payload ─────────────────────────
    kafka_df = build_alert_payload(tagged_df)

    # ── 5. Write alerts back to Kafka ─────────────────────────
    query = (
        kafka_df.writeStream
        .format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName("alert_detector")
        .start()
    )

    log.info(f"Alert detector query started — id={query.id}")
    log.info(f"Rules active: overspeed(>{T_SPEED}), engine_overheat(>{T_ENGINE_TEMP}°C), "
             f"low_fuel(<{T_FUEL}%), high_rpm(>{T_RPM}), hard_braking(<{T_HARD_BRAKE}m/s²), "
             f"idle_overheat(>{T_IDLE_TEMP}°C & speed<5)")

    # ── 6. Block + heartbeat ──────────────────────────────────
    while query.isActive:
        progress = query.lastProgress
        if progress:
            log.info(
                f"[heartbeat] batch={progress.get('batchId','?')} | "
                f"input={progress.get('numInputRows', 0)} rows | "
                f"duration={progress.get('durationMs',{}).get('triggerExecution', 0)}ms"
            )
        time.sleep(TRIGGER_SECONDS * 2)

    query.awaitTermination()


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("  Smart City — Alert Detector  (v2 — fixed)")
    log.info(f"  Kafka broker  : {KAFKA_BOOTSTRAP}")
    log.info(f"  Source topic  : {TOPIC_IN}")
    log.info(f"  Output topic  : {TOPIC_OUT}")
    log.info(f"  Trigger       : {TRIGGER_SECONDS}s")
    log.info(f"  Checkpoint    : {CHECKPOINT_DIR}")
    log.info("")
    log.info("  Thresholds:")
    log.info(f"    SPEED      > {T_SPEED} km/h       → HIGH")
    log.info(f"    ENGINE_TEMP> {T_ENGINE_TEMP} °C    → CRITICAL")
    log.info(f"    FUEL       < {T_FUEL} %           → MEDIUM")
    log.info(f"    RPM        > {T_RPM}              → HIGH")
    log.info(f"    HARD_BRAKE < {T_HARD_BRAKE} m/s²  → HIGH")
    log.info(f"    IDLE_HEAT  > {T_IDLE_TEMP}°C+spd<5→ HIGH")
    log.info("=" * 60)

    spark = build_spark()
    try:
        run(spark)
    except KeyboardInterrupt:
        log.warning("Interrupted — stopping alert detector")
    finally:
        spark.stop()
        log.info("Alert Detector stopped.")


if __name__ == "__main__":
    main()