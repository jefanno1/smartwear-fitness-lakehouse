# SmartWear Fitness Analytics Lakehouse

An end-to-end local data engineering pipeline simulating a cloud-native **Databricks Lakehouse Architecture**. This project processes high-velocity, time-series telemetry streams from wearable fitness devices using **PySpark** and **Delta Lake**, structured around the **Medallion Architecture** design pattern.

## 🏗️ Architecture & Data Flow

The project processes data through three distinct data quality zones, maintaining complete transactional integrity locally via Delta Lake's ACID compliance log tracking.

1. **Bronze (Landing Zone):** Ingests raw, unstructured event CSV streams capturing irregular user activity logs, corrupt fields (negative heart rates), and missing keys.
2. **Silver (Cleaned/Enriched):** Schema enforcement, uppercase standardization of device tracking metrics, removal of null user blocks, and execution of conditional logic to classify exertion into active `intensity_zones`.
3. **Gold (Business Aggregates):** Actionable analytics summaries optimized for BI dashboards or machine learning consumers, computing total active tracking duration logs and peak heart rates per user.

---

## 🛠️ Tech Stack & Engineering Practices

* **Processing Engine:** Apache Spark (PySpark) optimized for distributed data structures.
* **Storage Layer:** Delta Lake protocol providing ACID transactions, time travel ledger tracking, and optimized Parquet layouts.
* **Testing Framework:** `pytest` unit-testing framework enforcing schema validations and data transformation assertions prior to code integration.
* **SDLC Methodology:** AI-native code generation structured around explicit schema contracts to prevent execution hallucination.

---

## 📂 Repository Blueprint

```text
fitness_lakehouse/
│
├── storage/                    # Simulates Cloud Bucket Storage Layer (S3/ADLS)
│   ├── bronze/                 # Raw ingestion drop directory
│   ├── silver/                 # Standardized Delta tables (includes _delta_log/)
│   └── gold/                   # Optimized analytics aggregates
│
├── src/                        # Production Pipeline Core App
│   ├── transformations.py      # Isolated deterministic Spark functions
│   └── pipeline_runner.py      # Local orchestration engine 
│
└── tests/                      # Testing & Verification Layer
    └── test_transformations.py # Automated pytest execution checks
```

## 🚀 Installation & Execution
1. Prerequisites
Ensure your local machine has Java JDK 17, Apache Hadoop binaries (winutils.exe), and Python 3.11+ configured inside your environment paths.

2. Setup Virtual Environment
Bash
python -m venv venv
source venv/Scripts/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
3. Run Automated Quality Assurance (Unit Tests)
Validate code logic and schema enforcement assumptions before triggering processing jobs:

Bash
pytest
4. Execute the End-to-End Pipeline
Bash
python src/pipeline_runner.py