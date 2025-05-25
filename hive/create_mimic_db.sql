-- Create the mimic_db database
CREATE DATABASE IF NOT EXISTS mimic_db
COMMENT 'MIMIC-III database for cleaned parquet data'
LOCATION 'hdfs://namenode:9000/transformed_mimic';

-- Use the mimic_db database
USE mimic_db;

-- Create ADMISSIONS table
CREATE EXTERNAL TABLE IF NOT EXISTS admissions (
    row_id INT,
    subject_id INT,
    hadm_id INT,
    admittime TIMESTAMP,
    dischtime TIMESTAMP,
    deathtime TIMESTAMP,
    admission_type STRING,
    admission_location STRING,
    discharge_location STRING,
    insurance STRING,
    language STRING,
    religion STRING,
    marital_status STRING,
    ethnicity STRING,
    edregtime TIMESTAMP,
    edouttime TIMESTAMP,
    diagnosis STRING,
    hospital_expire_flag INT,
    has_chartevents_data INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create CALLOUT table
CREATE EXTERNAL TABLE IF NOT EXISTS callout (
    row_id INT,
    subject_id INT,
    hadm_id INT,
    submit_wardid INT,
    curr_wardid INT,
    curr_careunit STRING,
    callout_wardid INT,
    callout_service STRING,
    request_tele INT,
    request_resp INT,
    request_cdiff INT,
    request_mrsa INT,
    request_vre INT,
    callout_status STRING,
    callout_outcome STRING,
    discharge_wardid INT,
    acknowledge_status STRING,
    createtime TIMESTAMP,
    updatetime TIMESTAMP,
    acknowledgetime TIMESTAMP,
    outcometime TIMESTAMP,
    firstreservationtime TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/transformed_mimic/CALLOUT.parquet'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create ICUSTAYS table
CREATE EXTERNAL TABLE IF NOT EXISTS icustays (
    row_id INT,
    subject_id INT,
    hadm_id INT,
    icustay_id INT,
    dbsource STRING,
    first_careunit STRING,
    last_careunit STRING,
    first_wardid INT,
    last_wardid INT,
    intime TIMESTAMP,
    outtime TIMESTAMP,
    los DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/transformed_mimic/ICUSTAYS.parquet'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create LABEVENTS table
CREATE EXTERNAL TABLE IF NOT EXISTS labevents (
    row_id INT,
    subject_id INT,
    hadm_id INT,
    itemid INT,
    charttime TIMESTAMP,
    value STRING,
    valuenum DOUBLE,
    valueuom STRING,
    flag STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/transformed_mimic/LABEVENTS.parquet'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Create PATIENTS table
CREATE EXTERNAL TABLE IF NOT EXISTS patients (
    row_id INT,
    subject_id INT,
    gender STRING,
    dob TIMESTAMP,
    dod TIMESTAMP,
    dod_hosp TIMESTAMP,
    dod_ssn TIMESTAMP,
    expire_flag INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/transformed_mimic/PATIENTS.parquet'
TBLPROPERTIES ('parquet.compression'='SNAPPY');