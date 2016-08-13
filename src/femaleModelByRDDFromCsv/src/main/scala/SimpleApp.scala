/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

case class WhichCluster(cluster_id: Int)

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
    .load("/tmp/female_model.csv")

    df.registerTempTable("female_model")
    val goodDF = sqlContext.sql("select " +
      "pnum," +
      "days_to_lastlogin," +
      "weeks_to_lastlogin," +
      "max_distinct_chat_contacts_hourly," +
      "hour_to_max_chat_contacts," +
      "fraud_status," +
      "email_action," +
      "total_chat_days," +
      "total_chat," +
      "max_chat_per_receiver," +
      "mean_chat," +
      //      "sd_chat," +
      //      "max_chat," +
      "total_msg_days," +
      "total_msg," +
      "max_msg_per_receiver," +
      "mean_msg," +
      "sd_msg," +
      "max_msg," +
      "total_login_days," +
      "total_login," +
      "mean_login," +
      "sd_login," +
      "max_login" +
      " from female_model")
    df.printSchema()

    val parsedData = goodDF.map {
      case Row(
      pnum: Int,
      days_to_lastlogin: Int,
      weeks_to_lastlogin: Int,
      max_distinct_chat_contacts_hourly: Int,
      hour_to_max_chat_contacts: Int,
      fraud_status: Int,
      email_action: Int,
      total_chat_days: Int,
      total_chat: Int,
      max_chat_per_receiver: Double,
      mean_chat: Double,
      //      sd_chat: Double,
      //      max_chat: Double,
      total_msg_days: Int,
      total_msg: Int,
      max_msg_per_receiver: Double,
      mean_msg: Double,
      sd_msg: Double,
      max_msg: Int,
      total_login_days: Int,
      total_login: Int,
      mean_login: Double,
      sd_login: Double,
      max_login: Int
      )
      => (Vectors.dense(
        days_to_lastlogin,
        weeks_to_lastlogin,
        max_distinct_chat_contacts_hourly,
        hour_to_max_chat_contacts,
        fraud_status,
        email_action,
        total_chat_days,
        total_chat,
        max_chat_per_receiver,
        mean_chat,
        //        sd_chat,
        //        max_chat,
        total_msg_days,
        total_msg,
        max_msg_per_receiver,
        mean_msg,
        sd_msg,
        max_msg,
        total_login_days,
        total_login,
        mean_login,
        sd_login,
        max_login
      ))
    }.cache()


    val list = List(2)

    list.foreach(n => {
      val numClusters = n
      val numIterations = 20
      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      println("----------------------" + numClusters + "--------------------------")

      val resultRDD = clusters.predict(parsedData)
      val resultDF = resultRDD.map(r => WhichCluster(r)).toDF()

      val countDF = resultDF.groupBy("cluster_id").count()

      println(" PMML Model :\n" + clusters.toPMML)
      countDF.show

//      save(countDF, "cluster_in_test" + n)
    })
  }

  def save(dataFrame: DataFrame, table: String): Unit = {
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", pwd)
    props.setProperty("driver", driver)

    // create and save in table
    dataFrame.write.jdbc(url, table, props)
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
