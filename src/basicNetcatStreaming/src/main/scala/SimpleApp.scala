import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SimpleApp {
      def main(args: Array[String]) : Unit = {
        val conf = new SparkConf().setAppName("Sinmple Application").set("spark.driver.allowMultipleContexts", "true")
        val ssc = new StreamingContext(conf, Seconds(5))

        val lines = ssc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.toLowerCase.split(" "))
        val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCount.print()

        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
      }
}

/**
 * $ ./bin/spark-submit --master local[2] --driver-memory 1G --executor-memory 1G --jars "/tmp/elasticsearch-hadoop-2.3.2.jar" --class "SimpleApp" /tmp/netcat-spark-elastic_2.11-1.0.jar
  * ./bin/spark-submit --master local[2] --driver-memory 1G --executor-memory 1G --class "SimpleApp" target/scala-2.11/basic-netcat-streaming_2.11-1.0.jar
 */
