import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Imputer

object DataMart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataMart ETL")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
      val inputPath = args(0)
      val outputPath = if (args.length > 1) args(1) else "/app/processed"

      // Читаем CSV
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)

      val featureCols = Array("energy_100g", "fat_100g", "carbohydrates_100g", "proteins_100g", "sugars_100g")

      // Заменяем отрицательные на null
      var processed = df
      for (c <- featureCols) {
        processed = processed.withColumn(c, when(col(c) < 0, null).otherwise(col(c)))
      }

      // Impute пропусков средним
      val imputer = new Imputer()
        .setInputCols(featureCols)
        .setOutputCols(featureCols)
        .setStrategy("mean")
      processed = imputer.fit(processed).transform(processed)

      // Сохраняем только исходные числовые колонки + id, без features/scaled_features
      val toSaveCols = Array("id") ++ featureCols
      processed.select(toSaveCols.head, toSaveCols.tail: _*)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(outputPath)

      println(s"DATA_SAVED: true")
      println(s"OUTPUT_PATH: $outputPath")
    } finally {
      spark.stop()
    }
  }
}
