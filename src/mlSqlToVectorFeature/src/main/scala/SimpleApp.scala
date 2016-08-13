package org.apache.spark.examples.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}

object SimpleApp {
  def main(args: Array[String]) {

val conf = new SparkConf().setAppName("SimpleParamsExample")
val sc = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

val df = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://bigdata-master:3306/sample").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "female_training").option("user", System.getenv("MYSQL_USERNAME")).option("password", System.getenv("MYSQL_PASSWORD")).load()

df.registerTempTable("female_training")
val rowsDF = sqlContext.sql("select logins, activedays, activehours from female_training where activehours is not null")
val training = rowsDF.map{case Row(logins: Int, activedays: Int, activehours: Int) => LabeledPoint(1.0, Vectors.dense(logins.asInstanceOf[Int], activedays.asInstanceOf[Int], activehours.asInstanceOf[Int]))}

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

// Prepare test data.
val test = sc.parallelize(Seq(
  LabeledPoint(1.0, Vectors.dense(44, 28, 43)),
  LabeledPoint(0.0, Vectors.dense(0, 0, 0)),
  LabeledPoint(0.0, Vectors.dense(1, 1, 1)),
  LabeledPoint(1.0, Vectors.dense(128, 46, 119))))

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

/*
val assembler = new VectorAssembler().setInputCols(Array("bareNuclei",
  "blandChromatin", "clumpThickness", "marginalAdhesion", "mitoses",
  "normalNucleoli", "singleEpithelialCellSize", "uniformityOfCellShape",
  "uniformityOfCellSize")).setOutputCol("features")
val df_new = assembler.transform(df)
 */
