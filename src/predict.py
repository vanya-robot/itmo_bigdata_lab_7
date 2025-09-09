from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler


class Predictor:
    def __init__(self, model_path: str):
        self.model = KMeansModel.load(model_path)

    @staticmethod
    def _has_lab5_columns(df: DataFrame) -> bool:
        """Check if lab5 selected columns exist in dataframe"""
        lab5_selected = [
            'energy_100g', 'fat_100g', 'carbohydrates_100g',
            'proteins_100g', 'sugars_100g'
        ]
        return all(c in df.columns for c in lab5_selected)

    @staticmethod
    def _preprocess_lab5(df: DataFrame) -> DataFrame:
        """Preprocess lab5 data: handle negatives, impute missing, assemble, scale"""
        selected_columns = [
            'energy_100g', 'fat_100g', 'carbohydrates_100g',
            'proteins_100g', 'sugars_100g'
        ]

        # negative values -> null
        for column in selected_columns:
            if column in df.columns:
                df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

        # Impute missing with mean
        imputer = Imputer(inputCols=selected_columns, outputCols=selected_columns, strategy="mean")
        df = imputer.fit(df).transform(df)

        # Assemble to 'features'
        assembler = VectorAssembler(inputCols=selected_columns, outputCol="features")
        df = assembler.transform(df)

        # Scale to 'scaled_features'
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
        scaler_model = scaler.fit(df)
        df = scaler_model.transform(df)
        return df

    def infer(self, df: DataFrame) -> DataFrame:
        """Run preprocessing and inference using the loaded model"""

        # If lab5 columns present, preprocess
        if self._has_lab5_columns(df):
            df = self._preprocess_lab5(df)

        # If no 'scaled_features', try to assemble from feature_* columns
        if "scaled_features" not in df.columns:
            feature_cols = [c for c in df.columns if c.startswith("feature_")]
            if feature_cols:
                assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
                df = assembler.transform(df)

        # Perform prediction
        pred = self.model.transform(df)

        # Keep 'id' if present, plus prediction
        cols = ["id"] if "id" in pred.columns else []
        cols += [col("prediction").cast("int").alias("prediction")]
        return pred.select(*cols)
