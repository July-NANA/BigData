import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Main {
  val target="b"
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter {  _.contains(target) } }
      .map { (_, 1) }
      .keyBy(1)
      .timeWindow(Time.minutes(1), Time.seconds(3)) // 定义了一个滑动窗口，窗口大小为1分钟，每3秒滑动一次
      .sum(1)

    val num=counts.map(x=>"b出现的次数： "+x._2)
    num.print()

    env.execute("Window Stream WordCount")
    println("exit now!")
  }
}
