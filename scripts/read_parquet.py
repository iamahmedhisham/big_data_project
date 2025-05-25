from pyspark.sql import SparkSession

def main():
    # 1. Create a SparkSession
    spark = SparkSession.builder \
        .appName("ReadParquetExample") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # 2. Set log level to WARN to suppress INFO logs
    spark.sparkContext.setLogLevel("WARN")

    # 3. Read the Parquet files from an HDFS folder
    parquet_path = "hdfs://namenode:9000/transformed_mimic/ADMISSIONS.parquet"
    df = spark.read.parquet(parquet_path)

    # 4. Print the first 10 rows
    print("=== Showing top 10 rows from the Parquet dataset ===")
    df.show(10)

    # 5. Print record count
    record_count = df.count()
    print("Total number of rows: {}".format(record_count))

    # 6. Print schema
    df.printSchema()

    # 7. Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()