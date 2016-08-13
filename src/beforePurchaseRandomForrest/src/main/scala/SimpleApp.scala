/* SimpleApp.scala */

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.ini4j.Ini

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/before_p"
  val driver = "com.mysql.jdbc.Driver"

  var user: String = ""
  var pwd: String = ""

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val creds = new Ini(new File("/opt/bigdata/credentials.ini"))
    user = creds.get("local-db", "username")
    pwd = creds.get("local-db", "password")

    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", "profile").
      option("user", user).
      option("password", pwd).
      load()

    val data = df.map{
        case Row(pnum: Int, age: Int, profile_ethnicity: Int, access_request: Int, add_favourite: Int, wink: Int, profile_views_before_purchase: Int, paid: Int) =>
        LabeledPoint(paid.toDouble, Vectors.dense(age.toDouble, profile_ethnicity.toDouble, access_request.toDouble, add_favourite.toDouble, wink.toDouble, profile_views_before_purchase.toDouble))
    }.toDF()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
    val model = pipeline.fit(trainingData)
    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "label", "features").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("-----------Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("-----------Learned classification forest model:\n" + rfModel.toDebugString)
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
