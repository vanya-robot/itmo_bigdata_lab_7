from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, array
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

class Predictor:
    def __init__(self, model_path: str):
        self.model = KMeansModel.load(model_path)

    def infer(self, df: DataFrame) -> DataFrame:
        feature_cols = ['energy_100g','fat_100g','carbohydrates_100g','proteins_100g','sugars_100g']

        # Собираем scaled_features в Vector для KMeans
        array_to_vector_udf = udf(lambda arr: Vectors.dense(arr) if arr is not None else None, VectorUDT())
        df = df.withColumn(
            "scaled_features",
            array_to_vector_udf(array(*feature_cols))
        )

        pred = self.model.transform(df)

        cols = ['id'] if 'id' in pred.columns else []
        cols += [col('prediction').cast('int').alias('prediction')]
        return pred.select(*cols)
