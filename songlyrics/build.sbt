name := "songlyrics"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= {
  val sparkVer = "2.3.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "com.databricks" %% "spark-csv_2.11" % "1.5.0"
  )
}