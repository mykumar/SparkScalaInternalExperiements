package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}

object SimpleApp {
  case class TestPoint(max_volume: Int, email_action: Int, public_photo: Long, private_photo: Long, featured_photo: Long, hidden_photo: Long)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SimpleParamsExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // db related
    val dsl = "jdbc:mysql://bigdata-master:3306/ashley"
    val username = System.getenv("MYSQL_USERNAME")
    val pwd = System.getenv("MYSQL_PASSWORD")

    //cache_sample_female_list
    val df1 = sqlContext.read.format("jdbc").option("url", dsl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "cache_sample_female_list")
      .option("user", username)
      .option("password", pwd)
      .load()
    df1.registerTempTable("list")

    //cache_sample_female_chat_max_distinct_contacts_hourly
    val df2 = sqlContext.read.format("jdbc").option("url", dsl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "cache_sample_female_chat_max_distinct_contacts_hourly")
      .option("user", username)
      .option("password", pwd)
      .load()
    df2.registerTempTable("max_contacts")

    //cache_sample_female_mail_last_action
    val df3 = sqlContext.read.format("jdbc").option("url", dsl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "cache_sample_female_mail_last_action")
      .option("user", username)
      .option("password", pwd)
      .load()
    df3.registerTempTable("mail_last_action")

    //cache_sample_female_photo
    val df4 = sqlContext.read.format("jdbc").option("url", dsl)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "cache_sample_female_photo")
      .option("user", username)
      .option("password", pwd)
      .load()
    df4.registerTempTable("photo")

    val rowsDF = sqlContext.sql(
        "select l.is_host, if(mc.max_volume is null, 0, mc.max_volume), if(mlc.email_action is null, 0, mlc.email_action), if(p.public_photo is null, 0, p.public_photo), if(p.private_photo is null, 0, p.private_photo), if(p.featured_photo is null, 0, p.featured_photo), if(p.hidden_photo is null, 0, p.hidden_photo)" +
          " from list l" +
          " left join max_contacts mc on mc.pnum = l.pnum" +
          " left join mail_last_action mlc on mlc.pnum = l.pnum" +
          " left join photo p on p.pnum = l.pnum"
    )
    println("------------------------------------------------------------")
    rowsDF.printSchema()
    val parsedData = rowsDF.map {
      case Row(is_host: Long, max_volume: Long, email_action: Long, public_photo: Long, private_photo: Long, featured_photo: Long, hidden_photo: Long) =>
        LabeledPoint(is_host.asInstanceOf[Double], Vectors.dense(max_volume.asInstanceOf[Int], email_action.asInstanceOf[Int], public_photo.asInstanceOf[Int], private_photo.asInstanceOf[Int], featured_photo.asInstanceOf[Int], hidden_photo.asInstanceOf[Int]))
    }

    val splits = parsedData.randomSplit(Array(0.9, 0.1), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val lr = new LogisticRegression()
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    lr.setMaxIter(10).setRegParam(0.01)

    val model1 = lr.fit(training.toDF)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
    paramMap.put(lr.maxIter, 30) // Specify 1 Param.  This overwrites the original maxIter.
    paramMap.put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training.toDF, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    model2.transform(test.toDF)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }

    sc.stop()

  }
}
