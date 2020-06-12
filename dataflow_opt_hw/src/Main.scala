
import java.util.{Properties, UUID}

import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil

import scala.util.parsing.json.JSON

import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON


object Main {
  //s3参数
  val accessKey = "C802139A1D6D3356ABCD"
  val secretKey = "WzU0NDRGMDc1Q0I5OTA5QjI0QTk5RThENEU0RDY4MTI4MkNDM0M3ODJd"
  //s3地址
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  //上传到的桶
  val bucket = "fanyuhang"
  //要读取的文件
  val key = "daas.txt"

  //kafka参数
  val inputTopic = "mn_buy_ticket_demo2"
  //kafka地址
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"


  //上传文件的路径前缀
  val keyPrefix = "storage/"
  //上传数据间隔 单位毫秒
  val period = 5000
  //输入的kafka主题名称
//  val inputTopic = "ryccjl_1"


  def main(args: Array[String]): Unit = {
    //生产
    val s3Content = readFile()
    produceToKafka(s3Content)

    //对接
    duijie()

    //写入s3
    write()

  }




  /**
    * 从s3中读取文件内容
    *
    * @return s3的文件内容
    */
  def readFile(): String = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }

  /**
    * 把数据写入到kafka中
    *
    * @param s3Content 要写入的内容
    */
  def produceToKafka(s3Content: String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](inputTopic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }

  //对接
  def duijie():Unit={
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
//    det.writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period))
    env.execute()
  }

  //写入s3
  def write():Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
    inputKafkaStream.writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period))
    env.execute()

  }
}
