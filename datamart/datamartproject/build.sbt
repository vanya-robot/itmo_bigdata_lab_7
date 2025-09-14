val scala213Version = "2.13.16"
val sparkVersion = "4.0.0"
val hadoopVersion = "3.3.6"

lazy val root = project
  .in(file("."))
  .settings(
    name := "datamartproject",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala213Version,  // Теперь 2.13!

    // Настройки assembly
    assembly / assemblyJarName := "datamart-fat.jar",
    assembly / mainClass := Some("DataMart"),
    
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },

    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion
    )
  )