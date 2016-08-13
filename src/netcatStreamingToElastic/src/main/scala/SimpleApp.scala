import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
      def main(args: Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("Sinmple Application").set("spark.driver.allowMultipleContexts", "true")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(conf, Seconds(5))
        val lines = ssc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.toLowerCase.split(" "))
        words.foreachRDD { rdd =>
          val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
          import sqlContext.implicits._
          val wordsDataFrame = rdd.toDF("words")
          wordsDataFrame.registerTempTable("allwords")
          val wcdf = sqlContext.sql("select words,count(*) as total from allwords group by words")
          wcdf.show()
          import org.elasticsearch.spark.sql._
          wcdf.saveToEs("wordcount/wc")
        }
        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
      }
}

/**
 * $ ./bin/spark-submit --master local[2] --driver-memory 1G --executor-memory 1G --jars "/tmp/elasticsearch-hadoop-2.3.2.jar" --class "SimpleApp" /tmp/netcat-spark-elastic_2.11-1.0.jar
 */
