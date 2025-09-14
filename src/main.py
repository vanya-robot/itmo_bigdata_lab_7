import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import SparkConfig, AppConfig
from src.predict import Predictor
import time
import os

def run():
    # Загружаем конфиги
    app_cfg = AppConfig()
    spark_cfg = SparkConfig()
    spark = spark_cfg.get_spark_session()

    # Ждем немного для кэширования
    time.sleep(5)
    
    # Прямой доступ к кэшированной таблице
    table_name = "cached_datamart"
    
    if spark.catalog.tableExists(table_name) and spark.catalog.isCached(table_name):
        df = spark.table(table_name)
        print(f"Successfully retrieved cached table: {table_name}")
    else:
        raise Exception(f"Table {table_name} not found or not cached")
    
    # Основная логика
    predictor = Predictor(app_cfg.model_path)
    preds = predictor.infer(df)

    run_ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    # Сохраняем результаты
    output_path = os.getenv('HDFS_PREDICTIONS_PATH', '/app/preds.txt')
    if output_path.startswith('hdfs://'):
        preds.write.mode('overwrite').csv(output_path)
    else:
        preds.toPandas().to_csv(output_path, index=False)
    
    print(f"Results saved to: {output_path}")
    spark.stop()

if __name__ == "__main__":
    run()