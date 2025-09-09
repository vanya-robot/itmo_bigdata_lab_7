import sys
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config import SparkConfig, AppConfig
from src.predict import Predictor

def run():
    app_cfg = AppConfig()
    spark_cfg = SparkConfig()
    spark = spark_cfg.get_spark_session()

    # stdout Scala скрипта
    csv_data = sys.stdin.read()
    pdf = pd.read_csv(io.StringIO(csv_data))

    df = spark.createDataFrame(pdf)

    predictor = Predictor(app_cfg.model_path)
    preds = predictor.infer(df)

    run_ts = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    preds.show(10, False)

    preds.toPandas().to_csv("/app/preds.txt", index=False)

    spark.stop()

if __name__ == "__main__":
    run()
