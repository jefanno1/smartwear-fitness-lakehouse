# src/pipeline_runner.py
import os
import sys

# === ENVIRONMENT OVERRIDES ===
os.environ["SPARK_HOME"] = r"D:\zephaniah_I\Project Data Engineering\venv\Lib\site-packages\pyspark"
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = (os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"])

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
# Import BOTH transformation functions now
from transformations import process_bronze_to_silver, process_silver_to_gold

BRONZE_PATH = r"D:\zephaniah_I\Project Data Engineering\Project_pyspark\fitness_lakehouse\storage\bronze"
SILVER_PATH = r"D:\zephaniah_I\Project Data Engineering\Project_pyspark\fitness_lakehouse\storage\silver"
GOLD_PATH   = r"D:\zephaniah_I\Project Data Engineering\Project_pyspark\fitness_lakehouse\storage\gold"

def run_pipeline():
    builder = SparkSession.builder \
        .appName("SmartWear-Pipeline") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("\n📥 1. LANDING MESSY DATA IN THE BRONZE LAYER...")
    os.makedirs(BRONZE_PATH, exist_ok=True)
    raw_csv_file = os.path.join(BRONZE_PATH, "device_logs_2026_05_21.csv")
    raw_df = spark.read.csv(raw_csv_file, header=True, inferSchema=True)

    print("\n🛠️ 2. PROCESSING AND WRITING TO SILVER DELTA LAYER...")
    cleaned_df = process_bronze_to_silver(raw_df)
    cleaned_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print("✅ Silver Delta Layer Synced.")

    print("\n🌟 3. READING FROM SILVER TO CALCULATE GOLD BUSINESS METRICS...")
    # Production best practice: Load directly from the saved Delta directory to ensure data lineage
    loaded_silver_delta = spark.read.format("delta").load(SILVER_PATH)
    
    # Process through our new aggregation logic
    gold_analytics_df = process_silver_to_gold(loaded_silver_delta)
    gold_analytics_df.show()

    print(f"\n💾 4. COMMITTING SUMMARY REPORT TO THE GOLD LAYER VIA DELTA LAKE...")
    os.makedirs(GOLD_PATH, exist_ok=True)
    gold_analytics_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print(f"✅ Production Analytics Report generated successfully inside: {GOLD_PATH}")

    spark.stop()

if __name__ == "__main__":
    run_pipeline()