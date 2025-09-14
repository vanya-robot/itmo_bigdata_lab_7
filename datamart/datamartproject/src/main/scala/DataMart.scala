import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object DataMart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataMart ETL")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try {
      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args(0))

      val processed = df.select(df.columns.map(c =>
        when(col(c) === -1, null).otherwise(col(c)).alias(c)
      ): _*)

      // Регистрируем как временную таблицу и кэшируем
      processed.createOrReplaceTempView("cached_datamart")
      spark.sql("CACHE TABLE cached_datamart")
      
      // Ждем завершения кэширования
      spark.sql("SELECT COUNT(*) FROM cached_datamart").show()
      
      println("DATA_CACHED: true")
      println("TABLE_NAME: cached_datamart")
      
      // Долгое ожидание для Python скрипта
      Thread.sleep(60000)
      
    } finally {
      spark.stop()
    }
  }
}