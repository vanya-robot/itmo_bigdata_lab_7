import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataMart {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: DataMart <input_csv_hdfs_path>")
      sys.exit(1)
    }

    val inputPath = args(0)
    val spark = SparkSession.builder()
      .appName("DataMart ETL")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    // чтение CSV из HDFS
    val df = spark.read.option("header","true").option("inferSchema","true").csv(inputPath)

    // замена -1 на null
    val processed = df.select(df.columns.map(c =>
      when(col(c) === -1, null).otherwise(col(c)).alias(c)
    ): _*)

    // вывод результата в stdout в CSV
    processed.coalesce(1) // чтобы был один CSV
      .write.option("header", "true")
      .csv("/tmp/scala_output") // временный локальный файл

    // вывод в stdout
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputDir = new org.apache.hadoop.fs.Path("/tmp/scala_output")
    val files = fs.listStatus(outputDir).map(_.getPath.toString).filter(_.endsWith(".csv"))
    files.foreach { file =>
      val rdd = spark.sparkContext.textFile(file)
      rdd.collect().foreach(println)
    }

    spark.stop()
  }
}
