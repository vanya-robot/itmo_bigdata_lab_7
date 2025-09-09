import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Imputer, VectorAssembler, StandardScaler}

object DataMart {
  def main(args: Array[String]): Unit = {
    val input = if (args.length > 0) args(0) else "hdfs:///data/raw/source_data.csv"
    val output = if (args.length > 1) args(1) else "hdfs:///data/mart/processed.parquet"

    val spark = SparkSession.builder()
      .appName("DataMart")
      .getOrCreate()

    import spark.implicits._

    // Read raw CSV
    val raw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(input)

    // selected columns (lab5)
    val selected = Seq("energy_100g", "fat_100g", "carbohydrates_100g", "proteins_100g", "sugars_100g")

    // Replace negative values with null
    var df = raw
    for (c <- selected) {
      if (df.columns.contains(c)) {
        df = df.withColumn(c, when(col(c) < 0, lit(null).cast("double")).otherwise(col(c)))
      }
    }

    // Impute missing values with mean
    val imputer = new Imputer()
      .setInputCols(selected.toArray)
      .setOutputCols(selected.toArray)
      .setStrategy("mean")

    df = imputer.fit(df).transform(df)

    // Assemble features
    val assembler = new VectorAssembler()
      .setInputCols(selected.toArray)
      .setOutputCol("features")

    df = assembler.transform(df)

    // Scale features
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithMean(true)
      .setWithStd(true)

    val scalerModel = scaler.fit(df)
    val out = scalerModel.transform(df)

  // Load trained KMeans model and run inference on the processed DataFrame
  val modelPath = sys.env.getOrElse("MODEL_PATH", "src/models/kmeans_model")
  import org.apache.spark.ml.clustering.KMeansModel

  val kmeansModel = KMeansModel.load(modelPath)
  val predictions = kmeansModel.transform(out)

  // Show predictions (id + prediction + optional features preview)
  val colsToShow = Seq("id", "prediction") ++ (if (out.columns.contains("scaled_features")) Seq("scaled_features") else Seq())
  predictions.select(colsToShow.map(col): _*).show(50, truncate = false)

  spark.stop()
  }
}
