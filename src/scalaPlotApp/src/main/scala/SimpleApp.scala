/* SimpleApp.scala */
import org.sameersingh.scalaplot.Implicits._
import scala.math._

object SimpleApp {
  def main(args: Array[String]) {
    val x = 0.0 until 2.0 * Pi by 0.1
    output(PNG("/tmp/", "test"), xyChart(x ->(sin(_), cos(_))))
  }
}
