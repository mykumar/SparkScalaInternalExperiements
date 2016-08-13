import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

import org.myutils.Credentials

object SimpleApp {
  val url = "jdbc:mysql://bigdata-master:3306/nlp"
  val driver = "com.mysql.jdbc.Driver"

  var user: String = ""
  var pwd: String = ""

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val handler = new Credentials("/opt/bigdata/credentials.ini").handler()
    user = handler.get("local-db", "username")
    pwd = handler.get("local-db", "password")

    val df = sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("dbtable", "topics").
      option("user", user).
      option("password", pwd).
      load()

    // val corpus: RDD[String] = sc.wholeTextFiles("docs/*").map(_._2)
    val corpus: RDD[String] = df.map { case Row(document: String) => document }

    // Split each document into a sequence of terms (words)
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 2).filter(_.forall(java.lang.Character.isLetter)))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] = tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = (termCounts.length * 0.3).toInt
    val vocabArray: Array[String] = termCounts.takeRight(termCounts.length - numStopwords).map(_._1)

    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex().map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 10
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)

    // Print topics, showing top-weighted 3 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 3)
    topicIndices.foreach { case (terms, termWeights) =>
      println("TOPIC:")
      terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
      }
      println()
    }
  }
}
