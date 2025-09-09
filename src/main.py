import uuid
import time
from src.utils.logging import get_logger
from src.io_sources.postgres_io import PostgresIO
from src.predict import Predictor
from src.config import AppConfig, PostgresConfig, SparkConfig
from pyspark.sql import functions as F
import psycopg2


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
    predictor = Predictor(app_config.model_path)

    # run inference
    logger.info("Running inference")
    result_df = predictor.infer(df)

    run_id = str(uuid.uuid4())
    run_ts = time.strftime("%Y-%m-%d %H:%M:%S")

    # prepare predictions to save
    preds = result_df.withColumn("run_id", F.lit(run_id))
    preds = preds.withColumn("run_ts", F.lit(run_ts))

    # save predictions
    logger.info(f"Saving predictions to: {pg_config.table_predictions}")
    pg.write_table(preds, pg_config.table_predictions)

    logger.info("Prediction run completed")

    logger.info("Checking predictions table directly in Postgres")
    conn = psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        user=pg_config.user,
        password=pg_config.password,
        database=pg_config.database
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {pg_config.table_predictions};")
    rows = cur.fetchall()
    logger.info(f"Found {len(rows)} rows in predictions:")
    for row in rows:
        print(row)
    cur.close()
    conn.close()

    logger.info("Shutting down Spark session")
    spark.stop()


if __name__ == "__main__":
    run()