import java.sql.{Connection, DriverManager}
import java.util

import com.bingocloud.util.json.JSONObject

object test {
  def main(args: Array[String]): Unit = {


    val username = "user25"
    val password = "pass@bingo25"
    val drive = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user25_db?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false"
    val javaClass=new arr()
    val list=javaClass.queryAll()
    println(list.toString)
    val array=list.toString
    println("array: " + array.getClass)

//    println(list[1])
    for(i <- array){
      println(i)
    }
  }
}
