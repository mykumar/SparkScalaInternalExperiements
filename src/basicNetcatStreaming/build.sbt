name := "basic netcat streaming"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0",
  "org.apache.spark" % "spark-streaming_2.10" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.10" % "2.0.0"
)
