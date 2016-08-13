/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * [Note:]
 * 1. download two jars: spark-csv_2.10-1.3.0.jar, commons-csv-1.2.jar
 * 2. copy cars.csv to all of nodes
 */
object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/tmp/cars.csv")

    df.show()

    val selectedData = df.select("year", "model")
    selectedData
      .coalesce(1)   // merge all partitions as one in case result is distributely stored on all nodes
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/tmp/cars_new.csv")
  }
}
