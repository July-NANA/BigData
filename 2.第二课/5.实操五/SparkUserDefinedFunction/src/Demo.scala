import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Demo {

  case class Row(id: Int, name: String, sex: String, idCard: Long, pic: String)

  def main(args: Array[String]): Unit = {
    //spark相关配置信息
    val conf = new SparkConf().setAppName("Spark Sql Test").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext._
    import sqlContext.implicits._

    //设置读取的文件
    val row = sc.textFile("file:///E:/学习资料/大数据实训/资源/第二课/user_data.txt").map(_.split(",")).map(p => Row(p(0).toInt, p(1), p(2), p(3).toLong, p(4))).toDF()
    row.registerTempTable("row")
    spark.udf.register("mosaic", (str: String) => Utils.makePicMosaic(str))
    val result = sql("SELECT id, name, sex, idCard, pic, mosaic(pic) FROM row ORDER BY id")
//    result.foreach(x => {
//      //把处理之前的图片输出到input文件夹中
//      Utils.generateImage(x(4).toString, "E:/学习资料/大数据实训/第三课/input/" + System.nanoTime() + ".png")
//      //把马赛克处理后的文件输出到result文件夹中
//      Utils.generateImage(x(5).toString, "E:/学习资料/大数据实训/第三课/result/" + System.nanoTime() + ".png")
//    })
    //输出结果到控制台
    //result.map(t => "id:" + t(0) + " name:" + t(1) + " sex:" + t(2) + "  idCard:" + t(3)).collect().foreach(println)
    //result.map(t => "  idCard:" + t(3)).collect().foreach(println)
//    val idCard=result.map(t => "  idCard:" + t(3))
    val idCard=result.select("idCard")

//    val arr=idCard.collectAsList()
//    idCard.collect().foreach
    val rdd=idCard.rdd
    //脱敏
    rdd.foreach(x=>{
      Utils.tuomin(x.toString())
    })
    println("rdd: "+rdd.getClass)
    println("idCard: "+idCard.getClass)
    println("Result: "+result.getClass)
//    result.foreach(x=>println(x(3)))
    sc.stop()
  }
}
