from pyspark.sql import SparkSession
from config import POSTGRES, SPARK


def load_data():
    spark = (
        SparkSession.builder
        .appName(SPARK["app_name"])
        .master(SPARK["master"])
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{POSTGRES['host']}:{POSTGRES['port']}/{POSTGRES['db']}"
    properties = {
        "user": POSTGRES["user"],
        "password": POSTGRES["password"],
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table="input_data", properties=properties)
    return df, spark
