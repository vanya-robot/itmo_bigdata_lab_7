from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame
from src.config import PostgresConfig
from src.utils.logging import get_logger

logger = get_logger(__name__)

def jdbc_url(host: str, port: str, db: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{db}"

def _jdbc_props(user: str, password: str, schema: str) -> Dict[str, str]:
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "currentSchema": schema,
    }

class PostgresIO:
    def __init__(self, cfg: PostgresConfig):
        self.cfg = cfg

    def read_table(self, spark: SparkSession, table: str, predicates: Optional[list] = None) -> DataFrame:
        logger.info(f"Reading Postgres table {table} from {self.cfg.jdbc_url()}")
        df = spark.read.jdbc(url=self.cfg.jdbc_url(), table=table, properties=self.cfg.jdbc_properties())
        return df

    def write_table(self, df: DataFrame, table: str, mode: str = "append") -> None:
        logger.info(f"Writing DataFrame to Postgres table {table} (mode={mode})")
        df.write.jdbc(url=self.cfg.jdbc_url(), table=table, mode=mode, properties=self.cfg.jdbc_properties())
