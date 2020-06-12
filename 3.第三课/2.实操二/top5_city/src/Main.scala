import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.ReduceFunction

//import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.mutable.ListBuffer
//import org.json.{JSONArray, JSONObject}
import scala.util.parsing.json.JSON

object Main {
  /**
    * 输入的主题名称
    */
  val inputTopic = "mn_buy_ticket_demo2"
  /**
    * kafka地址
    */
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)

//    inputKafkaStream.print()
//    val det=inputKafkaStream.flatMap { _.toLowerCase.split(",") filter {  _.contains("destination") } }
//      .map { (_, 1) }
//      .keyBy(0)
//      .sum(1)


//        .map(x=>{
//          val jsonObj: JSONObject = JSON.parseObject(x)
//        })

    val det=inputKafkaStream.flatMap { _.toLowerCase.split("\t") }
        .map{
      x=>
        val j=JSON.parseFull(x) match {
          case Some(map: Map[String,Any])=>map
          case other=>Map("destination"->"error")
        }
        (j("destination").toString,x)
      }
      .keyBy(0)
      .reduce(new ReduceFunction[(String,String)]{
        override def reduce(value1:(String,String),value2:(String,String)):(String,String)={
          (value1._1,value1._2+"||"+value2._2)
        }
      })
    println("inputKafkaStream: "+inputKafkaStream.getClass)
    println("det: "+det.getClass)
//    det.map(x => println(x._1))
//    det.map(x => println(x._1.getClass))
//    det.map(x => println(x))
//    println(det)
//    det.print()
//    det.map(x=>println(x))
//    val sortedMap=det.order
//    val lie = det.map(t=>t._2.toInt)

//    val lst1 = new ListBuffer[Int]
//    lie.map(x=>lst1.append(x.toInt))
//    lie.map(x=>println(x))
//    println(lst1)

//    inputKafkaStream.map(x => println(x))
    env.execute()
  }


}
