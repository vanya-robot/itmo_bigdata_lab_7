name := "DataMartProject"

version := "0.1"

scalaVersion := "2.12.17" // для Spark 3.x

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1"
)

mainClass in Compile := Some("DataMart")
