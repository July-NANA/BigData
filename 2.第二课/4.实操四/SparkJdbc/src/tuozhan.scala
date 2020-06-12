import java.util.Properties

object tuozhan {

  def main(args: Array[String]): Unit = {
    import java.sql.DriverManager
    val url = "jdbc:postgresql://bigdata28.depts.bingosoft.net:25432/user25_db"
    val properties = new Properties()
    properties.setProperty("driverClassName", "org.postgresql.Driver")
    properties.setProperty("user", "user25")
    properties.setProperty("password", "pass@bingo25")

    val connection = DriverManager.getConnection(url, properties)

    val statement = connection.createStatement

    var resultSet = statement.executeQuery("select * from t_rk_jbxx limit 5")
    try {
      while (resultSet.next) {
        val mqsfzhm = resultSet.getString(1)
        val xm = resultSet.getString(4)
        val xb = resultSet.getString(6)
        val zxhm=resultSet.getString(8)

        //输出前五行
        println(s"$mqsfzhm      $xm   $xb   $zxhm")
      }
      resultSet.close()


    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
