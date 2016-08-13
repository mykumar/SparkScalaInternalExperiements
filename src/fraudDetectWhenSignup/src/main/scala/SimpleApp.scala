import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.myutils.{Credentials, DbSaver}

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/fraud"
  val driver = "com.mysql.jdbc.Driver"

  var user: String = ""
  var pwd: String = ""

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val handler = new Credentials("/opt/bigdata/credentials.ini").handler()
    user = handler.get("local-db", "username")
    pwd = handler.get("local-db", "password")

    val loadTable = {
      table: String =>
        sqlContext.read.format("jdbc").
          option("url", url).
          option("driver", driver).
          option("dbtable", table).
          option("user", user).
          option("password", pwd).
          load()
    }

    val df = loadTable("one_day")

    val data = df.map{
      case Row(pnum: Int, country: Int, gender: Int, seeking: Int, age: Int, ethnic: Int, vids: Int, vid_linked_fraud:Int, ips: Int, ip_linked_fraud: Int, emails:Int, email_linked_fraud:Int, is_fraud_domain: Int, is_same: Int, is_fraud: Int) =>
        LabeledPoint(is_fraud.toDouble, Vectors.dense(country.toDouble, gender.toDouble, seeking.toDouble, age.toDouble, ethnic.toDouble, vids.toDouble, vid_linked_fraud.toDouble, ips.toDouble, ip_linked_fraud.toDouble, emails.toDouble, email_linked_fraud.toDouble, is_fraud_domain: Int, is_same.toDouble))
    }.toDF()

    val layers = Array[Int](13, 10, 9, 8, 2)

    // -- start training data
    // val Array(trainingData, testData) = data.randomSplit(Array(0.5, 0.5))
    val trainingData = data

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    // train the model
    val model = trainer.fit(trainingData)

    // -- predict testing data
    val dfNew = loadTable("one_day_copy")
    val testData = dfNew.map{
      case Row(pnum: Int, country: Int, gender: Int, seeking: Int, age: Int, ethnic: Int, vids: Int, vid_linked_fraud:Int, ips: Int, ip_linked_fraud: Int, emails:Int, email_linked_fraud:Int, is_fraud_domain: Int, is_same: Int, is_fraud: Int) =>
        LabeledPoint(is_fraud.toDouble, Vectors.dense(country.toDouble, gender.toDouble, seeking.toDouble, age.toDouble, ethnic.toDouble, vids.toDouble, vid_linked_fraud.toDouble, ips.toDouble, ip_linked_fraud.toDouble, emails.toDouble, email_linked_fraud.toDouble, is_fraud_domain: Int, is_same.toDouble))
    }.toDF()

    val result = model.transform(testData)

    val dbSaver = new DbSaver(url, user, pwd, driver)
    val resultDF = result.select("prediction", "label").repartition(20)
    dbSaver.createAndSave(resultDF, "result")

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(resultDF))
  }
}
