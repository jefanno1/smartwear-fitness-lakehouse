# src/transformations.py
from pyspark.sql.functions import col, upper, when, to_timestamp
from pyspark.sql.functions import col, upper, when, to_timestamp, count, max as spark_max

# [Keep your existing process_bronze_to_silver function exactly as it is]
def process_bronze_to_silver(raw_df):
    """
    Cleans raw fitness tracker logs:
    1. Standardizes device names to UPPERCASE.
    2. Drops rows with null heart rates or invalid user IDs.
    3. Converts string timestamps into active Spark Timestamp objects.
    4. Categorizes exertion into intensity zones.
    """
    return raw_df \
        .dropna(subset=["user_id", "heart_rate"]) \
        .filter(col("heart_rate") > 0) \
        .withColumn("device_type", upper(col("device_type"))) \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn(
            "intensity_zone",
            when(col("heart_rate") < 120, "Fat Burn")
            .when((col("heart_rate") >= 120) & (col("heart_rate") < 160), "Cardio")
            .otherwise("Peak")
        )

def process_silver_to_gold(silver_df):
    """
    Aggregates Silver data into clean Gold business metrics:
    Calculates total active tracking logs and maximum heart rate recorded per user.
    """
    return silver_df \
        .groupBy("user_id") \
        .agg(
            count("timestamp").alias("total_active_logs"),
            spark_max("heart_rate").alias("peak_heart_rate")
        )