import time
from src.utils.logging import get_logger
from src.predict import Predictor
from src.config import SparkConfig, AppConfig
from pyspark.sql import functions as F

logger = get_logger(__name__)


def run():
    app_cfg = AppConfig()
    spark_cfg = SparkConfig()

    spark = spark_cfg.get_spark_session()

    input_path = app_cfg.processed_input_path or "hdfs:///data/mart/processed.parquet"
    output_path = app_cfg.predictions_output_path or "hdfs:///data/predictions/preds.parquet"

    logger.info(f"Reading processed data from {input_path}")
    df = spark.read.parquet(input_path)
    logger.info(f"Loaded {df.count()} rows of processed data")

    logger.info(f"Loading model from {app_cfg.model_path}")
    predictor = Predictor(app_cfg.model_path)

    logger.info("Running inference")
    preds = predictor.infer(df)

    run_ts = time.strftime("%Y-%m-%d %H:%M:%S")
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    logger.info(f"Saving predictions to {output_path}")
    preds.write.mode("overwrite").parquet(output_path)

    logger.info("Predictions sample:")
    preds.show(10, False)

    spark.stop()


if __name__ == "__main__":
    run()
