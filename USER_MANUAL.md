# User Manual: MIMIC-III Big Data Pipeline

## Introduction
This user manual provides detailed instructions for using the **MIMIC-III Big Data Pipeline**, a system for processing and analyzing the [MIMIC-III Clinical Database](https://physionet.org/content/mimiciii/) using **Apache Spark**, **Apache Hive**, and **Hadoop HDFS**. The pipeline cleans raw MIMIC-III CSV files, converts them to Parquet, stores them in HDFS, creates a Hive database (`mimic_db`), and enables querying for research or analytics.

This guide is intended for researchers, data analysts, or developers with basic knowledge of Docker, Python, and SQL. It covers setup, data preparation, pipeline execution, querying, and troubleshooting.

## Project Components
- **Docker Environment**: Runs Spark, Hive, and HDFS containers (`namenode`, `hive-server`, `spark-master`).
- **Scripts**: Python scripts for cleaning (`clean_mimic_tables.py`), schema extraction (`extract_schemas.py`), Hive table creation (`create_hive_tables.py`), querying (`query_admissions.py`), and validation (`validate_data.py`).
- **SQL**: Hive scripts (`create_mimic_db.sql`, `queries.sql`) for database setup and sample queries.
- **HDFS Storage**: Stores cleaned Parquet files at `hdfs://namenode:9000/transformed_mimic`.

## Prerequisites
Before starting, ensure you have:
- **Docker** and **Docker Compose** installed ([Docker Desktop](https://www.docker.com/products/docker-desktop/) for Windows).
- **Windows** (for `run_py_into_spark.bat`; Linux/Mac users can adapt commands).
- **Python 3.6+** (for PySpark scripts, compatible with Spark 3.0.0).
- **Git** to clone the repository.
- **MIMIC-III Data**: CSV files from PhysioNet (requires [access approval](https://physionet.org/content/mimiciii/)).
- At least **8GB RAM** and **20GB disk space** for Docker containers and data.

## Installation
Follow these steps to set up the pipeline.

### 1. Clone the Repository
Clone the project to your local machine:
```bash
git clone https://github.com/iamahmedhisham/big_data_project.git
cd big_data_project
```

### 2. Obtain MIMIC-III Data
1. Request access to MIMIC-III via [PhysioNet](https://physionet.org/content/mimiciii/).
2. Download the CSV files (`ADMISSIONS.csv`, `CALLOUT.csv`, `ICUSTAYS.csv`, `LABEVENTS.csv`, `PATIENTS.csv`).
3. Store them locally (e.g., `C:\mimic_data\`).

### 3. Start the Docker Environment
1. Navigate to the `docker/` directory:
   ```bash
   cd docker
   ```
2. Start the containers:
   ```bash
   docker-compose up -d
   ```
   This launches:
   - `namenode`: HDFS (port `9000`)
   - `hive-server`: Hive (port `10000`)
   - `hive-metastore`: Hive metastore (port `9083`)
   - `spark-master`: Spark (ports `7077`, `8080`)
   - `spark-worker`: Spark worker

3. Verify containers are running:
   ```bash
   docker ps
   ```

### 4. Configure HDFS
1. Create directories for raw and transformed data:
   ```bash
   docker exec namenode hdfs dfs -mkdir -p hdfs://namenode:9000/mimic_db
   docker exec namenode hdfs dfs -mkdir -p hdfs://namenode:9000/transformed_mimic
   docker exec namenode hdfs dfs -mkdir -p hdfs://namenode:9000/user/hive/warehouse
   ```
2. Set permissions:
   ```bash
   docker exec namenode hdfs dfs -chmod -R 777 hdfs://namenode:9000/
   ```
3. Upload MIMIC-III CSV files to HDFS:
   ```bash
   docker cp C:\mimic_data\*.csv namenode:/tmp/
   docker exec namenode hdfs dfs -put /tmp/*.csv hdfs://namenode:9000/mimic_db/
   ```

## Using the Pipeline
The pipeline consists of three main steps: cleaning data, creating Hive tables, and querying.

### 1. Clean MIMIC-III Data
The `clean_mimic_tables.py` script cleans CSV files and saves them as Parquet.

1. Run the cleaning script (Windows):
   ```bash
   cd scripts
   run_py_into_spark.bat
   ```
   This executes `clean_mimic_tables.py`, which:
   - Reads CSVs from `hdfs://namenode:9000/mimic_db/[TABLE].csv`.
   - Cleans data (e.g., converts timestamps, handles nulls).
   - Saves Parquet files to `hdfs://namenode:9000/transformed_mimic/[TABLE].parquet`.

2. Verify Parquet files:
   ```bash
   docker exec namenode hdfs dfs -ls hdfs://namenode:9000/transformed_mimic
   ```

### 2. Extract Schemas (Optional)
To view the schemas of Parquet files:
1. Copy and run `extract_schemas.py`:
   ```bash
   docker cp scripts/extract_schemas.py spark-master:/tmp/data_folder/extract_schemas.py
   docker exec spark-master spark-submit /tmp/data_folder/extract_schemas.py
   ```
2. Output shows schemas for `ADMISSIONS`, `CALLOUT`, etc., e.g.:
   ```text
   === Schema for ADMISSIONS ===
   root
    |-- row_id: integer (nullable = true)
    |-- subject_id: integer (nullable = true)
    ...
   ```

### 3. Create Hive Database and Tables
The `create_hive_tables.py` script creates the `mimic_db` database and external tables linked to Parquet files.

1. Copy and run the script:
   ```bash
   docker cp scripts/create_hive_tables.py spark-master:/tmp/data_folder/create_hive_tables.py
   docker exec spark-master spark-submit /tmp/data_folder/create_hive_tables.py
   ```
2. This executes `sql/create_mimic_db.sql`, creating tables like:
   ```sql
   CREATE EXTERNAL TABLE mimic_db.admissions (
       row_id INT,
       subject_id INT,
       ...
   )
   STORED AS PARQUET
   LOCATION 'hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet';
   ```

3. Verify tables:
   ```bash
   docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
   ```
   ```sql
   USE mimic_db;
   SHOW TABLES;
   ```

### 4. Query the Data
Query the `mimic_db` tables using Hive or Spark SQL.

#### Option 1: Hive via Beeline
1. Access Beeline:
   ```bash
   docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
   ```
2. Run a query:
   ```sql
   USE mimic_db;
   SELECT * FROM admissions LIMIT 5;
   ```
   Example output:
   ```text
   +---------+-------------+----------+------------+...
   | row_id  | subject_id  | hadm_id  | admittime  |...
   +---------+-------------+----------+------------+...
   | 1       | 2           | 163353   | 2138-07-17 |...
   ...
   ```

#### Option 2: Spark SQL
1. Run `query_admissions.py`:
   ```bash
   docker cp scripts/query_admissions.py spark-master:/tmp/data_folder/query_admissions.py
   docker exec spark-master spark-submit /tmp/data_folder/query_admissions.py
   ```
2. This executes:
   ```sql
   SELECT * FROM mimic_db.admissions LIMIT 5;
   ```

#### Sample Queries
- Count expired patients:
  ```sql
  SELECT COUNT(*) FROM admissions WHERE hospital_expire_flag = 1;
  ```
- Join admissions and patients:
  ```sql
  SELECT a.subject_id, a.admittime, p.gender
  FROM admissions a
  JOIN patients p ON a.subject_id = p.subject_id
  LIMIT 10;
  ```

### 5. Validate Data
Run `validate_data.py` to check data integrity:
```bash
docker cp scripts/validate_data.py spark-master:/tmp/data_folder/validate_data.py
docker exec spark-master spark-submit /tmp/data_folder/validate_data.py
```
This verifies row counts, nulls, and schema consistency.

## Troubleshooting
- **HDFS File Not Found**:
  Ensure CSVs are in `hdfs://namenode:9000/mimic_db`:
  ```bash
  docker exec namenode hdfs dfs -ls hdfs://namenode:9000/mimic_db
  ```
- **Hive Query Errors**:
  - Use `LIMIT 5` (not `LIMIT(5)`).
  - Check metastore:
    ```bash
    docker logs hive-metastore
    ```
- **Spark Logs Too Verbose**:
  Logs are set to `WARN` in scripts. Modify `log4j.properties` (if added) for more control.
- **Docker Issues**:
  Restart containers:
  ```bash
  docker-compose down
  docker-compose up -d
  ```

## FAQs
- **Where do I get MIMIC-III data?**
  Apply for access at [PhysioNet](https://physionet.org/content/mimiciii/).
- **Can I run this on Linux/Mac?**
  Yes, replace `run_py_into_spark.bat` with a shell script:
  ```bash
  docker cp scripts/clean_mimic_tables.py spark-master:/tmp/data_folder/
  docker exec spark-master spark-submit /tmp/data_folder/clean_mimic_tables.py
  ```
- **How do I add new tables?**
  Update `clean_mimic_tables.py` and `create_mimic_db.sql` with new table schemas.

## Additional Resources
- **MIMIC-III Documentation**: [physionet.org](https://physionet.org/content/mimiciii/)
- **Spark**: [spark.apache.org](https://spark.apache.org/docs/3.0.0/)
- **Hive**: [hive.apache.org](https://hive.apache.org/)
- **Docker**: [docs.docker.com](https://docs.docker.com/)

## Contact
For support, open an issue on [GitHub](https://github.com/iamahmedhisham/big_data_project) or contact [iamahmedhisham](https://github.com/iamahmedhisham).
