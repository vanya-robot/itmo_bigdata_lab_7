import os
from dataclasses import dataclass
from pyspark.sql import SparkSession
from dataclasses import field, dataclass


@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "postgres")
    port: int = int(os.getenv("POSTGRES_PORT", 5432))
    database: str = os.getenv("POSTGRES_DB", "lab_db")
    user: str = os.getenv("POSTGRES_USER", "lab_user")
    password: str = os.getenv("POSTGRES_PASSWORD", "lab_pass")
    table_source: str = os.getenv("POSTGRES_TABLE_SOURCE", "source_data")
    table_predictions: str = os.getenv("POSTGRES_TABLE_PREDICTIONS", "predictions")

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def jdbc_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


@dataclass
class SparkConfig:
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", "1g")
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
    driver_cores: str = os.getenv("SPARK_DRIVER_CORES", "1")
    executor_cores: str = os.getenv("SPARK_EXECUTOR_CORES", "1")
    num_executors: str = os.getenv("SPARK_NUM_EXECUTORS", "1")
    java_opts: str = os.getenv("SPARK_JAVA_OPTS", "-Dfile.encoding=UTF-8")
    jdbc_jar: str = os.getenv("POSTGRES_JDBC_PATH", "/opt/bitnami/spark/jars/postgresql-42.5.0.jar")

    def get_spark_session(self) -> SparkSession:
        builder = SparkSession.builder.master(self.master).appName("lab6_predictor")
        # set memory/cores
        builder = builder.config("spark.driver.memory", self.driver_memory)
        builder = builder.config("spark.executor.memory", self.executor_memory)
        builder = builder.config("spark.driver.cores", self.driver_cores)
        builder = builder.config("spark.executor.cores", self.executor_cores)
        builder = builder.config("spark.executor.instances", self.num_executors)
        builder = builder.config("spark.jars", self.jdbc_jar)
        builder = builder.config("spark.driver.extraJavaOptions", self.java_opts)
        builder = builder.config("spark.executor.extraJavaOptions", self.java_opts)
        return builder.getOrCreate()


@dataclass
class AppConfig:
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    model_path: str = os.getenv("MODEL_PATH", "src/models/kmeans_model")