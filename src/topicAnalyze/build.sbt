name := "topic analyze"

version := "1.0"

scalaVersion := "2.10.5" //changed 2.11.7 due to scala version in Spark is 2.10.5

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.0",
  "org.apache.spark" % "spark-sql_2.10"  % "1.6.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0",
  "org.ini4j" % "ini4j" % "0.5.4"
)
