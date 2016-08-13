/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/nlp"
  val driver = "com.mysql.jdbc.Driver"
  val user = System.getenv("MYSQL_USERNAME")
  val pwd = System.getenv("MYSQL_PASSWORD")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", "msg").
      option("user", user).
      option("password", pwd).
      load()
    df.registerTempTable("t_msg")

    val msgDF = sqlContext.sql("select message from t_msg")
    msgDF.printSchema()

    val cleaner = (msg: String) => {
      msg.toLowerCase.split(" ").map((w: String) => w.replaceAll("[^a-zA-Z0-9]", "")).distinct
    }
    val wordDF = msgDF.explode("message", "word")((r: String) => cleaner(r))

    wordDF.registerTempTable("words")
    val wordCount = sqlContext.sql("select word, count(1) as cnt from words group by word order by cnt desc")
    println(wordCount.count())

    save(wordCount, "msg_word_count")
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
