import java.util.Properties

object Demo {
  def main(args: Array[String]): Unit = {
    import java.sql.DriverManager
    val url = "jdbc:hive2://bigdata36.depts.bingosoft.net:22236/user25_db"
    val properties = new Properties()
    properties.setProperty("driverClassName", "org.apache.hive.jdbc.HiveDriver")
    properties.setProperty("user", "user25")
    properties.setProperty("password", "pass@bingo25")

    val connection = DriverManager.getConnection(url, properties)

    val statement = connection.createStatement

    var resultSet = statement.executeQuery("show tables")
    try {
      while (resultSet.next) {
        val tableName = resultSet.getString(1)
        //输出所有表名
        println("tableName：" + tableName)
      }
      resultSet.close()

      //查询t_rk_jbxx表结构
      resultSet = statement.executeQuery("desc formatted t_rk_jbxx")
      while (resultSet.next) {
        val columnName = resultSet.getString(1)
        val columnType = resultSet.getString(2)
        val comment = resultSet.getString(3)
        //输出表结构
        println(s"columnName：$columnName     columnType:$columnType     comment:$comment")
      }
      resultSet.close()

      //查询t_rk_jbxx表数据
      println("表数据：")
      resultSet = statement.executeQuery("select * from t_rk_jbxx limit 10")
      while (resultSet.next) {
        val asjbh = resultSet.getString(4)
        val ajmc = resultSet.getString(1)
        val bamjbh = resultSet.getString(8)
        //输出表数据

        println(s"$asjbh    ｜$ajmc    ｜$bamjbh")
      }
      resultSet.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
