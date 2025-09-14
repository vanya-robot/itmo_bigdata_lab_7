from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class Predictor:
    def __init__(self, model_path: str):
        self.model = KMeansModel.load(model_path)

    def infer(self, df: DataFrame) -> DataFrame:
        """Assumes df already contains 'scaled_features' produced by the DataMart.
        Outputs DataFrame with columns ['id','prediction'] where prediction is int.
        """
        if 'scaled_features' not in df.columns:
            raise ValueError("Input DataFrame must contain 'scaled_features' produced by the DataMart")

        pred = self.model.transform(df)
        cols = ['id'] if 'id' in pred.columns else []
        cols += [col('prediction').cast('int').alias('prediction')]
        return pred.select(*cols)
