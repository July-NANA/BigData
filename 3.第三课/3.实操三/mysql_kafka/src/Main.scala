 import java.sql.{Connection, DriverManager}
 import java.util.Properties

 import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
 import org.nlpcn.commons.lang.util.IOUtil

object Main {

  val username = "user25"
  val password = "pass@bingo25"
  val drive = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user25_db?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false"

  //kafka参数
  val topic = "mn_buy_ticket_1"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    val s3Content = readFile()
    produceToKafka(s3Content)

  }

  def readFile(): String={

    val javaClass=new arr()
    val list=javaClass.queryAll()
    println(list.toString)
    val array=list.toString
    return array
//    var connection: Connection = null
//    try {
//      //在spark中如果不写会出错
//      classOf[com.mysql.jdbc.Driver]
//      connection = DriverManager.getConnection(url, username, password)
//      val statement = connection.createStatement()
//      val resultSet = statement.executeQuery("show tables")
//      //      while (resultSet.next()) {
//      //        println(resultSet.getString("city"))
//      //      }
//      println("resultSet: " + resultSet.getClass)
//      val result = resultSet.toString
//      println(result)
//      println("resultSet: " + resultSet.getClass)
//      return result;
//    }

  }

  def produceToKafka(s3Content:String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("},")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }

}
