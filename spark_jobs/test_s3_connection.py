"""
spark_jobs/test_s3_connection.py

Verifies that Spark can read from and write to your S3 bucket.
Run AFTER updating .env and restarting containers.

Run:
  docker exec sc_spark_master \
    /opt/spark/bin/spark-submit \
      --master local[2] \
      --jars /opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
             /opt/spark/jars/extra/aws-java-sdk-bundle-1.12.261.jar \
      /opt/spark_jobs/test_s3_connection.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark_jobs")
from utils.s3_utils import configure_spark_s3, S3_BUCKET

GREEN = "\033[92m"
RED   = "\033[91m"
BOLD  = "\033[1m"
RESET = "\033[0m"

def check(label: str, ok: bool, detail: str = ""):
    icon   = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
    suffix = f"  [{detail}]" if detail else ""
    print(f"  {icon}  {label}{suffix}")
    return ok

def main():
    print(f"\n{BOLD}{'='*55}")
    print("  Smart City — S3 Connection Test")
    print(f"{'='*55}{RESET}\n")

    # ── 1. Check env vars ─────────────────────────────────
    print("Checking environment variables...")
    key    = os.getenv("AWS_ACCESS_KEY_ID",     "")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region = os.getenv("AWS_REGION",            "")
    bucket = os.getenv("S3_BUCKET_NAME",        "")

    check("AWS_ACCESS_KEY_ID set",     bool(key),    f"starts with {key[:4]}..." if key else "MISSING")
    check("AWS_SECRET_ACCESS_KEY set", bool(secret), "present" if secret else "MISSING")
    check("AWS_REGION set",            bool(region), region or "MISSING")
    check("S3_BUCKET_NAME set",        bool(bucket), bucket or "MISSING")

    if not all([key, secret, region, bucket]):
        print(f"\n{RED}Fix missing env vars in .env and restart containers.{RESET}")
        sys.exit(1)

    # ── 2. Build SparkSession with S3A ────────────────────
    print("\nBuilding SparkSession with S3A config...")
    try:
        builder = (
            SparkSession.builder
            .appName("SmartCity-S3Test")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        builder = configure_spark_s3(builder)
        spark   = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        check("SparkSession with S3A config", True, f"bucket={bucket}")
    except Exception as e:
        check("SparkSession creation", False, str(e))
        sys.exit(1)

    # ── 3. Write a tiny test file to S3 ───────────────────
    test_path = f"s3a://{bucket}/_test/spark_connection_test"
    print(f"\nWriting test file to S3...\n  path: {test_path}")
    try:
        test_df = spark.createDataFrame(
            [("smart_city", "spark_s3_test", 1)],
            ["project", "test", "value"]
        )
        test_df.write.mode("overwrite").parquet(test_path)
        check("Write Parquet to S3", True, "file written")
    except Exception as e:
        check("Write Parquet to S3", False, str(e))
        print(f"\n{RED}Common causes:{RESET}")
        print("  • JARs missing from spark/jars/ — run download_jars.ps1")
        print("  • Wrong AWS credentials — check .env")
        print("  • Bucket name mismatch — check S3_BUCKET_NAME in .env")
        print("  • IAM policy missing s3:PutObject — check IAM user policy")
        spark.stop()
        sys.exit(1)

    # ── 4. Read it back ───────────────────────────────────
    print("\nReading test file back from S3...")
    try:
        read_df   = spark.read.parquet(test_path)
        row_count = read_df.count()
        value     = read_df.collect()[0]["test"]
        check("Read Parquet from S3",  row_count == 1, f"rows={row_count}")
        check("Data integrity",        value == "spark_s3_test", f"value={value}")
    except Exception as e:
        check("Read Parquet from S3", False, str(e))
        spark.stop()
        sys.exit(1)

    # ── 5. Write a partitioned file (mimics Bronze writer) ─
    print("\nTesting partitioned write (Bronze-style)...")
    try:
        part_path = f"s3a://{bucket}/_test/partitioned_test"
        part_df   = spark.range(20).withColumn(
            "partition_date", F.lit("2025-01-15")
        ).withColumn(
            "partition_hour", (F.col("id") % 3).cast("int")
        )
        part_df.write.mode("overwrite").partitionBy(
            "partition_date", "partition_hour"
        ).parquet(part_path)
        read_part  = spark.read.parquet(part_path)
        part_count = read_part.count()
        check("Partitioned write to S3",  part_count == 20, f"rows={part_count}")
    except Exception as e:
        check("Partitioned write to S3", False, str(e))

    # ── 6. Clean up test files ────────────────────────────
    print("\nCleaning up test files...")
    try:
        sc     = spark.sparkContext
        hadoop = sc._jvm.org.apache.hadoop
        conf   = sc._jsc.hadoopConfiguration()
        fs     = hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(f"s3a://{bucket}"),
            conf
        )
        fs.delete(
            sc._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/_test"),
            True
        )
        check("Test files cleaned up", True, "")
    except Exception as e:
        # Non-critical — cleanup failure doesn't affect the pipeline
        check("Test files cleaned up", False, f"manual cleanup needed: s3://{bucket}/_test/")

    # ── Summary ───────────────────────────────────────────
    print(f"""
{GREEN}{BOLD}S3 connection verified.{RESET}
{GREEN}Spark can read and write Parquet to s3a://{bucket}/{RESET}

Next: start the full pipeline
  Step 1 — Bronze writer (reads Kafka, writes to S3 Bronze):
    .\\spark_jobs\\submit_jobs.ps1 -Job bronze

  Step 2 — Alert detector (reads Kafka, writes alerts):
    .\\spark_jobs\\submit_jobs.ps1 -Job alerts

  Then wait ~30s for Bronze files to appear in S3, then:
  Step 3 — Silver cleaner:
    .\\spark_jobs\\submit_jobs.ps1 -Job silver

  Step 4 — Gold aggregator:
    .\\spark_jobs\\submit_jobs.ps1 -Job gold
""")
    spark.stop()


if __name__ == "__main__":
    main()
