# tests/test_transformations.py
import os
import sys
import pytest

# === ENVIRONMENT OVERRIDES (Ensures pytest isolates Spark correctly) ===
os.environ["SPARK_HOME"] = r"D:\zephaniah_I\Project Data Engineering\venv\Lib\site-packages\pyspark"
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] = (os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"])

# Dynamically link the src directory so the test can locate transformations.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pyspark.sql import SparkSession
from transformations import process_bronze_to_silver

# 1. Setup a reusable local Spark session for the testing session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Pipeline-Unit-Testing") \
        .master("local[2]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

# 2. Write the actual logic test
def test_process_bronze_to_silver(spark):
    # Step A: Create a controlled mock input dataframe (1 clean adult, 1 bad row, 1 negative HR)
    mock_data = [
        ("usr_01", "2026-05-21 08:00:00", "fitbit", 130), # Should pass -> Cardio
        (None,     "2026-05-21 08:05:00", "garmin", 140), # Should drop -> null user
        ("usr_02", "2026-05-21 08:10:00", "apple",  -5)   # Should drop -> negative heart rate
    ]
    schema = ["user_id", "timestamp", "device_type", "heart_rate"]
    input_df = spark.createDataFrame(mock_data, schema=schema)
    
    # Step B: Run the transformation logic
    output_df = process_bronze_to_silver(input_df)
    results = output_df.collect()
    
    # Step C: Assertions (Validations)
    # The 2 corrupt rows should have been filtered out completely
    assert len(results) == 1, f"Expected 1 clean record, but got {len(results)}"
    
    # Check that formats are properly standardized
    assert results[0]["device_type"] == "FITBIT", "Device type text failed to convert to uppercase."
    assert results[0]["intensity_zone"] == "Cardio", "Intensity tier evaluation logic was incorrect."
    assert results[0]["user_id"] == "usr_01"