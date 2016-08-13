/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/ashley"
  val driver = "com.mysql.jdbc.Driver"
  val user = System.getenv("MYSQL_USERNAME")
  val pwd = System.getenv("MYSQL_PASSWORD")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("/tmp/paid_mont.csv")

    df.registerTempTable("paid")
    val ttc = sqlContext.sql("select pnum, count(1) as times, min(p_month) as first_month, max(p_month) as last_month, sum(paid) as total from paid group by pnum").cache()

    ttc.registerTempTable("total_times")
    val result = sqlContext.sql("select * from total_times where (times> 10 or total > 350) and last_month >= '2015-07' and last_month < '2015-11'")

    result.collect().foreach(println)
    println("--------total count: " + result.count())

    sc.stop()
  }

  def save(dataFrame: DataFrame, table: String): Unit = {
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", pwd)
    props.setProperty("driver", driver)

    // create and save in table
    dataFrame.write.jdbc(url, table, props)
  }

  /**
   * used to calculate the value of the RDD at a specific percentile.
   * eg: calculatePercentile(rdd, 90)
   */
  def calculatePercentile(data: RDD[Double], tile: Double): Double = {
    val r = data.sortBy(x => x)
    val c = r.count()
    if (c == 1) r.first()
    else {
      val n = (tile / 100d) * (c + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
  }
}


/*
if table already exist
---------------------------------------------------------------------------------------------
sqlContext.read.format("jdbc").
        option("url", url).
        option("driver", driver).
        option("dbtable", "cluster_in_test").
        option("user", user).
        option("password", pwd).
        load()

org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(dataFrame, url, table, props)
 */
