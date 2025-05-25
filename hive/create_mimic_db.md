## Setting Up Hive Database for MIMIC-III with DBeaver
This section explains how to configure a Hive database (mimic_db) for the MIMIC-III project using DBeaver with a Beeline JDBC connection to create external tables stored as Parquet files in HDFS (hdfs://namenode:9000/transformed_mimic).

## Hive Database Configuration
The mimic_db database uses external Hive tables, each linked to a Parquet file in HDFS, allowing Hive to manage metadata while preserving data in HDFS.

## Key Features
 **Database**: mimic_db is stored in HDFS at hdfs://namenode:9000/transformed_mimic.
 
 **External Tables**: Tables (admissions, callout, icustays, labevents, patients) are external, ensuring Parquet files persist if tables are dropped.
 
  **Schema Mapping**:
   - Spark integer → Hive INT
   - Spark string → Hive STRING
   - Spark timestamp → Hive TIMESTAMP
   - Spark double → Hive DOUBLE

**Storage**: Tables use Parquet format with Snappy compression (TBLPROPERTIES ('parquet.compression'='SNAPPY')). Adjust to GZIP if needed.

**Nullable Columns**: All columns are nullable, matching the Parquet schemas.

**HDFS Locations**: Each table points to its respective Parquet file, e.g., hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet.

## Creating the Database with DBeaver
1- **Set Up DBeaver Connection:**

    - Install DBeaver (dbeaver.io).
    
    - Create a new database connection:
    
          - Database > New Connection > Select Apache Hive.
          
          **Configure**:
          
              - JDBC URL: jdbc:hive2://hive-server:10000/default
              
              - Username: Leave blank or use hive (default).
              
              - Password: Leave blank (unless configured).
              
              - Driver: Download the Hive JDBC driver if prompted.
              
              - Test the connection to ensure hive-server is accessible.
              
          - Test the connection to ensure hive-server is accessible.
          
2- **Execute SQL Script:**

- Open sql/create_mimic_db.sql from the repository in DBeaver’s SQL editor.
- Execute the script to create mimic_db and its tables:

Example snippet from create_mimic_db.sql

```text
 CREATE DATABASE IF NOT EXISTS mimic_db

 LOCATION 'hdfs://namenode:9000/transformed_mimic';

 USE mimic_db;

 CREATE EXTERNAL TABLE IF NOT EXISTS admissions (
   row_id INT, subject_id INT,
    ...
 )
 STORED AS PARQUET

 LOCATION 'hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet';

**Verify table creation:**

SHOW TABLES IN mimic_db;

## Setting Up Hive in Docker

To support Beeline JDBC connections, configure a hive-server container in your Docker environment, alongside Spark and HDFS.

**Prerequisites**

  - Docker and Docker Compose installed.
  - Project directory: C:\Users\ICT012\Desktop\big_data_project.
  - HDFS running at namenode:9000.

## Testing the Setup

- In DBeaver, query a table to verify:
  
  SELECT * FROM mimic_db.admissions LIMIT 5;

