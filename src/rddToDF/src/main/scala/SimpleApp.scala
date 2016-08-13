/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

case class Person(name: String, age: Int)

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    val infoRDD = sc.parallelize(info)
    val people = infoRDD.map(r => Person(r._1, r._2)).toDF()

    people.registerTempTable("people")

    val subDF = sqlContext.sql("select * from people where age > 30")

    subDF.show()
  }
}
