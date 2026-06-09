"""
spark_jobs/verify_bronze.py
Read Bronze Parquet files and print sample rows + row counts.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyBronze") \
    .master("local[2]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

topics = {
    "vehicle-telemetry": ["vehicle_id", "speed_kmh", "engine_temp_c", "fuel_level_pct", "road_type"],
    "traffic-events":    ["latitude", "longitude", "traffic_density", "congestion_ratio", "source"],
    "road-events":       ["vehicle_id", "event_type", "road_type", "latitude", "longitude"],
}

for topic, cols in topics.items():
    print(f"\n{'='*55}")
    print(f"  TOPIC: {topic}")
    print(f"{'='*55}")
    try:
        df = spark.read.parquet(f"/tmp/bronze/{topic}")
        print(f"  Row count : {df.count()}")
        print(f"  Columns   : {len(df.columns)}")
        print(f"  Partitions: {df.select('partition_date','partition_hour').distinct().count()} hour(s)")
        df.select(*cols).show(5, truncate=False)
    except Exception as e:
        print(f"  No data yet: {e}")

spark.stop()
print("\nBronze verification complete.")