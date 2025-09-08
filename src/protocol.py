from dataclasses import dataclass
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from datetime import datetime
import uuid

# Схема исходных данных (пример из ЛР5 openfoodfacts можно адаптировать под свои признаки)
INPUT_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("feature_1", DoubleType(), True),
    StructField("feature_2", DoubleType(), True),
    StructField("feature_3", DoubleType(), True),
    StructField("feature_4", DoubleType(), True),
])

# Схема результата
# prediction: int/str в зависимости от модели
RESULT_COLUMNS = ["id", "prediction", "run_id", "run_ts"]

@dataclass(frozen=True)
class RunMeta:
    run_id: str
    run_ts: str

    @staticmethod
    def new() -> "RunMeta":
        return RunMeta(run_id=str(uuid.uuid4()), run_ts=datetime.utcnow().isoformat(timespec="seconds") + "Z")