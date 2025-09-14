import os
from src.config import SparkConfig, AppConfig

def upload_to_hdfs():
    app_cfg = AppConfig()
    spark_cfg = SparkConfig()
    spark = spark_cfg.get_spark_session()

    print(f"Читаем локальный файл: {app_cfg.source_file}")
    df = spark.read.option("header", "true").csv(app_cfg.source_file)

    print(f"Записываем в HDFS: {app_cfg.hdfs_path}")
    # перезаписываем файл в HDFS
    df.write.mode("overwrite").option("header", "true").csv(app_cfg.hdfs_path)

    print("Файл успешно загружен в HDFS")

if __name__ == "__main__":
    upload_to_hdfs()
