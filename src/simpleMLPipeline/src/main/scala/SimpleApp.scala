package org.apache.spark.examples.ml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._    //{StructType,StructField,StringType};

import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val people = Array(
	"1,michael zhou spark,1.0",
	"2,Li spark,1.0",
	"3,Liawat spark  aww,1.0",
	"4,spark is a tool,1.0",
	"5,i love spark and use it,1.0",
	"6,spark is future of analytics,1.0",
	"6,hi spark @@,1.0",
	"7,who is it,0.0",
	"8,ttime to lunch,0.0",
	"9,this is a sone,0.0",
	"10,John test no,0.0"
    )
    val peopleTest = Array(
	"1,sweic a spark",
	"2,wdawd  wa dwa ww asxvrt sw",
	"3,no way to find me"
    )
    var peopleRDD = sc.parallelize(people)
    var peopleTestRDD = sc.parallelize(peopleTest)

    val schema = StructType(Seq(
    	StructField("id", LongType, false),
    	StructField("name", StringType, false),
    	StructField("label", DoubleType, false)
    ))
    val schemaTest = StructType(Seq(
    	StructField("id", LongType, false),
    	StructField("name", StringType, false)
    ))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = peopleRDD.map(_.split(",")).map(p => Row(p(0).toLong, p(1), p(2).trim.toDouble))
    val rowTestRDD = peopleTestRDD.map(_.split(",")).map(p => Row(p(0).toLong, p(1).trim))

    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    val peopleTestDataFrame = sqlContext.createDataFrame(rowTestRDD, schemaTest)

    val tokenizer = new Tokenizer()
                        .setInputCol("name")
                        .setOutputCol("words")
    val hashingTF = new HashingTF()
                        .setNumFeatures(1000)
                        .setInputCol(tokenizer.getOutputCol)
                        .setOutputCol("features")
    val lr = new LogisticRegression()
  			.setMaxIter(10)
  			.setRegParam(0.01)
    val pipeline = new Pipeline()
  			.setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(peopleDataFrame)

    model.transform(peopleTestDataFrame)
  			.select("id", "name", "probability", "prediction")
  			.collect()
  			.foreach { 
				case Row(id: Long, name: String, prob: Vector, prediction: Double) =>
    				println(s"($id, $name) --> prob=$prob, prediction=$prediction")
  			}
    sc.stop()
  }
}
