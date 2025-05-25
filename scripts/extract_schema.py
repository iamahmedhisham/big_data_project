from pyspark.sql import SparkSession

def main():
    # 1. Create a SparkSession
    spark = SparkSession.builder \
        .appName("ExtractParquetSchemas") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # 2. Set log level to WARN to suppress INFO logs
    spark.sparkContext.setLogLevel("WARN")

    # 3. List of tables
    tables = ["ADMISSIONS", "CALLOUT", "ICUSTAYS", "LABEVENTS", "PATIENTS"]

    # 4. Read and print schema for each table
    for table in tables:
        parquet_path = "hdfs://namenode:9000/transformed_mimic/{}.parquet".format(table)
        try:
            print("\n=== Schema for {} ===".format(table))
            df = spark.read.parquet(parquet_path)
            df.printSchema()
        except Exception as e:
            print("Error reading table {}: {}".format(table, str(e)))

    # 5. Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()