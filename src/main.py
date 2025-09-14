import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import SparkConfig, AppConfig
from src.predict import Predictor
import time
import os

def run():
    app_cfg = AppConfig()
    spark_cfg = SparkConfig()
    spark = spark_cfg.get_spark_session()

    # Путь, куда Scala сохранила обработанный CSV
    table_path = os.getenv("HDFS_PROCESSED_PATH", "processed")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(table_path)
    print(f"Loaded processed table from: {table_path}")

    predictor = Predictor(app_cfg.model_path)
    preds = predictor.infer(df)

    run_ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    # Показываем результаты предсказания
    print("=== Predictions Preview ===")
    preds.show(4, truncate=False)

    output_path = os.getenv('HDFS_PREDICTIONS_PATH', 'preds.txt')
    if output_path.startswith('hdfs://'):
        preds.write.mode('overwrite').csv(output_path)
    else:
        preds.toPandas().to_csv(output_path, index=False)

    print(f"Results saved to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    run()