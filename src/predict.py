from pyspark.ml.clustering import KMeansModel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler


def load_model(path: str) -> KMeansModel:
    return KMeansModel.load(path)


def _has_lab5_columns(df: DataFrame) -> bool:
    # lab5 selected columns
    lab5_selected = [
        'energy_100g', 'fat_100g', 'carbohydrates_100g',
        'proteins_100g', 'sugars_100g'
    ]
    return all(c in df.columns for c in lab5_selected)


def _preprocess_lab5(df: DataFrame) -> DataFrame:
    selected_columns = [
        'energy_100g', 'fat_100g', 'carbohydrates_100g',
        'proteins_100g', 'sugars_100g'
    ]

    # negative values -> null
    for column in selected_columns:
        if column in df.columns:
            df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

    # Impute missing with mean for all selected columns
    imputer = Imputer(inputCols=selected_columns, outputCols=selected_columns, strategy="mean")
    df = imputer.fit(df).transform(df)

    # Assemble to 'features'
    assembler = VectorAssembler(inputCols=selected_columns, outputCol="features")
    df = assembler.transform(df)

    # Scale to 'scaled_features' (fit on incoming data)
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    df = scaler_model.transform(df)
    return df


def infer(model: KMeansModel, df: DataFrame) -> DataFrame:
    # If lab5 columns present, create 'scaled_features'
    if _has_lab5_columns(df):
        df = _preprocess_lab5(df)

    # If model expects 'scaled_features', ensure it exists; otherwise try to assemble from feature_*
    if "scaled_features" not in df.columns:
        # try assembling from feature_* columns into 'features'
        feature_cols = [c for c in df.columns if c.startswith("feature_")]
        if feature_cols:
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            df = assembler.transform(df)

    # Perform prediction; keep 'id' if present
    pred = model.transform(df)
    cols = ["id"] if "id" in pred.columns else []
    cols += [col("prediction").cast("int").alias("prediction")]
    return pred.select(*cols)