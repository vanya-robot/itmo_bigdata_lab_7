import uuid
import time
from src.utils.logging import get_logger
from src.io_sources.postgres_io import PostgresIO
from src.predict import load_model, infer
from src.config import AppConfig, PostgresConfig, SparkConfig
from pyspark.sql import functions as F


logger = get_logger(__name__)


def run():
    pg_config = PostgresConfig()
    app_config = AppConfig()
    spark_config = SparkConfig()

    pg = PostgresIO(pg_config)

    logger.info("Starting prediction run")
    spark = spark_config.get_spark_session()

    # read source data
    logger.info(f"Reading source table: {pg_config.table_source}")
    df = pg.read_table(spark, pg_config.table_source)

    logger.info(f"Loaded {df.count()} rows from Postgres")

    # load model
    logger.info(f"Loading model from: {app_config.model_path}")
    model = load_model(app_config.model_path)

    # run inference
    logger.info("Running inference")
    result_df = infer(model, df)

    run_id = str(uuid.uuid4())
    run_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # prepare predictions to save
    preds = result_df.withColumn("run_id", F.lit(run_id))
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    # save predictions
    logger.info(f"Saving predictions to: {pg_config.table_predictions}")
    pg.write_table(preds, pg_config.table_predictions)

    logger.info("Prediction run completed")

    # keep application alive for inspection
    logger.info("Application will remain running for inspection. To stop, terminate the container.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down")
        spark.stop()


if __name__ == "__main__":
    run()