/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StructField, StructType}


object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Loads data
    val rowRDD = sc.textFile("/tmp/lda_data.txt").filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))
    val schema = StructType(Array(StructField("name", new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)
    dataset.show()

    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol("name")
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)
  }
}
