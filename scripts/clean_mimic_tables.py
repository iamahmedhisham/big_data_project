import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit
)
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
DEFAULT_TIMESTAMP = "9999-12-31 00:00:00"
DEFAULT_INT = -1
DEFAULT_STRING = "Unknown"

def clean_admissions(spark, input_path, output_path):
    """
    Clean the MIMIC-III ADMISSIONS table and save it as Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to input CSV file.
        output_path (str): Path to save output Parquet file.
    """
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        expected_columns = ["subject_id", "hadm_id", "admittime", "dischtime"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("Missing expected columns in {}: {}".format(input_path, expected_columns))

        df = df.dropna(how="all").dropna(subset=["subject_id", "hadm_id", "admittime", "dischtime"])

        for colname in ["admittime", "dischtime", "deathtime", "edregtime", "edouttime"]:
            if colname in df.columns:
                df = df.withColumn(colname, to_timestamp(col(colname)))

        df = df.withColumn("language", when(col("language").isNull(), DEFAULT_STRING).otherwise(col("language")))
        df = df.withColumn("religion", when(col("religion").isNull(), DEFAULT_STRING).otherwise(col("religion")))
        df = df.withColumn("marital_status", when(col("marital_status").isNull(), DEFAULT_STRING).otherwise(col("marital_status")))
        df = df.withColumn("edregtime", when(col("edregtime").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("edregtime")))
        df = df.withColumn("edouttime", when(col("edouttime").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("edouttime")))

        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully cleaned and saved ADMISSIONS to {}".format(output_path))
    except Exception as e:
        logger.error("Error processing {}: {}".format(input_path, str(e)))
        raise

def clean_callout(spark, input_path, output_path):
    """
    Clean the MIMIC-III CALLOUT table and save it as Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to input CSV file.
        output_path (str): Path to save output Parquet file.
    """
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how="all")
        expected_columns = ["createtime", "updatetime"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("Missing expected columns in {}: {}".format(input_path, expected_columns))

        for time_col in ["createtime", "updatetime", "acknowledgetime", "outcometime", "firstreservationtime"]:
            if time_col in df.columns:
                df = df.withColumn(time_col, to_timestamp(col(time_col)))

        df = df.drop("submit_careunit", "currentreservationtime")
        df = df.withColumn("discharge_wardid", when(col("discharge_wardid").isNull(), DEFAULT_INT).otherwise(col("discharge_wardid")))
        df = df.withColumn("acknowledgetime", when(col("acknowledgetime").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("acknowledgetime")))
        df = df.withColumn("firstreservationtime", when(col("firstreservationtime").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("firstreservationtime")))

        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully cleaned and saved CALLOUT to {}".format(output_path))
    except Exception as e:
        logger.error("Error processing {}: {}".format(input_path, str(e)))
        raise

def clean_icustays(spark, input_path, output_path):
    """
    Clean the MIMIC-III ICUSTAYS table and save it as Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to input CSV file.
        output_path (str): Path to save output Parquet file.
    """
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how="all")
        expected_columns = ["intime", "outtime"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("Missing expected columns in {}: {}".format(input_path, expected_columns))

        df = df.withColumn("intime", to_timestamp(col("intime")))
        df = df.withColumn("outtime", to_timestamp(col("outtime")))

        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully cleaned and saved ICUSTAYS to {}".format(output_path))
    except Exception as e:
        logger.error("Error processing {}: {}".format(input_path, str(e)))
        raise

def clean_labevents(spark, input_path, output_path):
    """
    Clean the MIMIC-III LABEVENTS table and save it as Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to input CSV file.
        output_path (str): Path to save output Parquet file.
    """
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how="all")
        expected_columns = ["charttime", "hadm_id"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("Missing expected columns in {}: {}".format(input_path, expected_columns))

        df = df.withColumn("charttime", to_timestamp(col("charttime")))
        df = df.withColumn("hadm_id", when(col("hadm_id").cast("int").isNotNull(), col("hadm_id").cast("int")).otherwise(DEFAULT_INT))
        df = df.withColumn("charttime", when(col("charttime").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("charttime")))
        df = df.fillna({
            "flag": "Normal",
            "valueuom": DEFAULT_STRING,
            "value": DEFAULT_STRING,
            "valuenum": DEFAULT_INT
        })

        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully cleaned and saved LABEVENTS to {}".format(output_path))
    except Exception as e:
        logger.error("Error processing {}: {}".format(input_path, str(e)))
        raise

def clean_patients(spark, input_path, output_path):
    """
    Clean the MIMIC-III PATIENTS table and save it as Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to input CSV file.
        output_path (str): Path to save output Parquet file.
    """
    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how="all")
        expected_columns = ["dob", "dod"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("Missing expected columns in {}: {}".format(input_path, expected_columns))

        df = df.withColumn("dob", to_timestamp(col("dob")))
        df = df.withColumn("dod", to_timestamp(col("dod")))
        df = df.withColumn("dod_hosp", to_timestamp(col("dod_hosp")))
        df = df.withColumn("dod_ssn", to_timestamp(col("dod_ssn")))
        df = df.withColumn("dod_hosp", when(col("dod_hosp").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("dod_hosp")))
        df = df.withColumn("dod_ssn", when(col("dod_ssn").isNull(), to_timestamp(lit(DEFAULT_TIMESTAMP))).otherwise(col("dod_ssn")))

        df.write.mode("overwrite").parquet(output_path)
        logger.info("Successfully cleaned and saved PATIENTS to {}".format(output_path))
    except Exception as e:
        logger.error("Error processing {}: {}".format(input_path, str(e)))
        raise

def main():
    """
    Main function to clean MIMIC-III tables and save them as Parquet.
    """
    spark = SparkSession.builder.appName("MIMIC-III Unified Cleaner").getOrCreate()

    try:
        clean_admissions(spark, "hdfs://namenode:9000/mimic_db/ADMISSIONS.csv", "hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet")
        clean_callout(spark, "hdfs://namenode:9000/mimic_db/CALLOUT.csv", "hdfs://namenode:9000/transformed_mimic/CALLOUT.parquet")
        clean_icustays(spark, "hdfs://namenode:9000/mimic_db/ICUSTAYS.csv", "hdfs://namenode:9000/transformed_mimic/ICUSTAYS.parquet")
        clean_labevents(spark, "hdfs://namenode:9000/mimic_db/LABEVENTS.csv", "hdfs://namenode:9000/transformed_mimic/LABEVENTS.parquet")
        clean_patients(spark, "hdfs://namenode:9000/mimic_db/PATIENTS.csv", "hdfs://namenode:9000/transformed_mimic/PATIENTS.parquet")
        logger.info("All MIMIC-III tables cleaned and saved as Parquet.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()