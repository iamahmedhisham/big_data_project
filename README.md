# MIMIC-III Big Data Pipeline

This repository implements a **big data pipeline** for processing the [MIMIC-III Clinical Database](https://github.com/physionet/db) using **Apache Spark**, **Apache Hive**, and **Hadoop HDFS**. It cleans raw MIMIC-III CSV files, converts them to Parquet, stores them in HDFS, creates a Hive database (`mimic_db`) with external tables, and supports querying the data for analysis, or research.

## Project Overview
- **Objective**: Transform and analyze MIMIC-III data (`ADMISSIONS`, `CALLOUT`, `ICUSTAYS`, `LABEVENTS`, `PATIENTS`) for research or analytics.
- **Tools**:
  - **Spark**: Data cleaning and transformation (PySpark).
  - **HDFS**: Storage of cleaned Parquet files (`hdfs://namenode:9000/transformed_mimic`).
  - **Hive**: Database management and querying.
  - **Docker**: Containerized environment for reproducibility.
- **Workflow**:
  1. Clean MIMIC-III CSV files and save as Parquet.
  2. Create Hive database and tables linked to Parquet files.
  3. Query data via Hive or Spark SQL.

## Data Flow Diagram

![Data Flow](./data_flow_diagram.png)

## Repository Structure
```plaintext
MIMIC-III-ETL-Pipeline/
├── docker-hadoop-spark/
│   └── Makefile
│   └── docker-compose.yml
│   └── entrypoint.sh
│   └── hadoop-hive.env
│   └── hadoop.env
│   └── spark_in_action.MD
│   └── startup.sh
├── hive/
│   └── hive_queries/
│      └── Admission_Type.sql
│      └── Lab_Results_by_Gender.sql
│      └── avg_stay_by_diagnosis.sql
│      └── hospital_deaths.sql
│      └── icu_readmission_distribution.sql
│      └── mortality_rate_by_age.sql
│   └── core-site.xml
│   └── hive-site.xml
│   └── create_mimic_db.sql
│   └── creating_external_tables.md
├── mimic_db/
│   └── dataset.md
├── Documentations/
│   └── architecture.md
│   └── data_flow_diagram.png
│   └── setup_guide.md
│   └── troubleshooting.md
│   └── usage_examples.md
├── scripts/
│   ├── copy_to_hdfs.bat
│   └── clean_mimic_tables.py
│   └── read_parquet.py
│   └── run_py_into_spark.bat
│   └── extract_schema.py
├── full_tables.png
├── README.md
└── LICENSE
```

## Prerequisites
- **Docker** and **Docker Compose** (for running Spark, Hive, HDFS).
- **Windows** (for `run_py_into_spark.bat`; Linux/Mac can use equivalent shell scripts).
- **MIMIC-III Data**: Access to MIMIC-III CSV files (requires [PhysioNet approval](https://physionet.org/content/mimiciii/)).
- **Git**: To clone the repository.
- **Python 3.6+**: For PySpark scripts (Spark 3.0.0 compatible).

## Setup Instructions
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/iamahmedhisham/big_data_project.git
   cd big_data_project
   ```

2. **Prepare MIMIC-III Data**:
   - Download MIMIC-III CSV files from PhysioNet (requires access approval).
   - Upload CSVs to HDFS:
     ```bash
     docker exec namenode hdfs dfs -mkdir -p hdfs://namenode:9000/mimic_db
     docker exec namenode hdfs dfs -put /path/to/mimic_csv/*.csv hdfs://namenode:9000/mimic_db/
     ```

3. **Start Docker Cluster**:
   ```bash
   cd docker
   docker-compose up -d
   ```
   This starts:
   - `namenode`: HDFS (port `9000`)
   - `hive-server`: Hive (port `10000`)
   - `hive-metastore`: Hive metastore (port `9083`)
   - `spark-master`: Spark (ports `7077`, `8080`)
   - `spark-worker`: Spark worker

4. **Initialize Hive Warehouse**:
   ```bash
   docker exec namenode hdfs dfs -mkdir -p hdfs://namenode:9000/user/hive/warehouse
   docker exec namenode hdfs dfs -chmod -R 777 hdfs://namenode:9000/user/hive
   ```

## Running the Pipeline
1. **Clean Data**:
   Convert MIMIC-III CSV files to Parquet:
   ```bash
   cd scripts
   run_py_into_spark.bat
   ```
   This runs `clean_mimic_tables.py`, saving Parquet files to `hdfs://namenode:9000/transformed_mimic/[TABLE].parquet`.

2. **Extract Schemas** (Optional):
   View Parquet schemas:
   ```bash
   docker cp scripts/extract_schemas.py spark-master:/tmp/data_folder/extract_schemas.py
   docker exec spark-master spark-submit /tmp/data_folder/extract_schemas.py
   ```

3. **Create Hive Tables**:
   Create `mimic_db` and external tables:
   ```bash
   docker cp scripts/create_hive_tables.py spark-master:/tmp/data_folder/create_hive_tables.py
   docker exec spark-master spark-submit /tmp/data_folder/create_hive_tables.py
   ```

4. **Query Data**:
   - Via Beeline:
     ```bash
     docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
     ```
     ```sql
     USE mimic_db;
     SELECT * FROM admissions LIMIT 5;
     ```
   - Via Spark SQL:
     ```bash
     docker cp scripts/query_admissions.py spark-master:/tmp/data_folder/query_admissions.py
     docker exec spark-master spark-submit /tmp/data_folder/query_admissions.py
     ```

5. **Validate Data**:
   Check Parquet data integrity:
   ```bash
   docker cp scripts/validate_data.py spark-master:/tmp/data_folder/validate_data.py
   docker exec spark-master spark-submit /tmp/data_folder/validate_data.py
   ```

## Example Query
```sql
USE mimic_db;
SELECT subject_id, hadm_id, admittime
FROM admissions
WHERE hospital_expire_flag = 1
LIMIT 10;
```

## Troubleshooting
- **HDFS Access**:
  Verify Parquet files:
  ```bash
  docker exec namenode hdfs dfs -ls hdfs://namenode:9000/transformed_mimic
  ```
- **Hive Errors**:
  Check metastore logs:
  ```bash
  docker logs hive-metastore
  ```
- **Spark Logs**:
  Set `log4j` to `WARN` in `clean_mimic_tables.py`:
  ```python
  spark.sparkContext.setLogLevel("WARN")
  ```

## Future Improvements
- Add Jupyter notebooks for EDA (`notebooks/`).
- Include configuration files (`config/hive-site.xml`, `core-site.xml`).
- Write detailed documentation (`docs/setup.md`, `pipeline.md`).
- Add unit tests for scripts (`scripts/tests/`).

## License
MIT License (see `LICENSE` file, to be added).

## Contact
For questions, open an issue or contact [iamahmedhisham](https://github.com/iamahmedhisham).
