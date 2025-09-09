import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 3:
        print("Usage: uploader.py <local_csv_path> <hdfs_target_path>")
        sys.exit(1)

    local_path = sys.argv[1]
    hdfs_path = sys.argv[2]

    spark = SparkSession.builder.appName("CSVUploader").getOrCreate()

    print(f"Reading local CSV from {local_path}")
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    print(f"Writing DataFrame to HDFS path {hdfs_path}")
    df.write.mode("overwrite").parquet(hdfs_path)

    print("Upload finished.")
    spark.stop()

if __name__ == "__main__":
    main()
