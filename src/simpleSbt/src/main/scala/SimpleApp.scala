/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://bigdata-master:3306/sample").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "aminno_log_affiliate_clickthrough").option("user", "tester").option("password", "Password@1").load()
    df.registerTempTable("log_clicks")
    val someRows = sqlContext.sql("select referrer, count(1) as cnt from log_clicks group by referrer order by cnt desc").take(20)

    println(someRows.mkString(" "))
  }
}
