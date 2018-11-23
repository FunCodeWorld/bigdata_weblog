package cn.bigdata.weblog

import java.text.SimpleDateFormat
import java.util.{Locale, UUID}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import util.control.Breaks._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object WebLogPre {
  val format1 = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss", Locale.US)
  val format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

  //定义请求页面必须是以下路径, 用于过滤静态资源
  var list = Seq[String]("/about","/black-ip-list/","/cassandra-clustor/"
    ,"/finance-rhive-repurchase/","/hadoop-family-roadmap/","/hadoop-hive-intro/"
    ,"/hadoop-zookeeper-intro/", "/hadoop-mahout-roadmap/")

  def main(args: Array[String]): Unit = {
    //1. 获取SparkContext对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WebLogPre")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //2. 读取文件
    val file: RDD[String] = sc.textFile(args(0))
    //60.208.6.156 - - [18/Sep/2013:06:49:48 +0000] "GET /wp-content/uploads/2013/07/rcassandra.png HTTP/1.0" 200 185524 "http://cos.name/category/software/packages/" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36"

    //3. 将每一行按照空格切分, 过滤出总长度>11的记录
    val lineArr: RDD[Array[String]] = file.map(_.split(" ")).filter(_.length > 11)

    //4. 将每个字段进行格式化, 然后把每一行作为一个元祖输出
    val lineRDD: RDD[(AnyVal, String, String, String, String, String, String, String, String)] = lineArr.map(arr => {
      //这条记录是否有效
      var valid = true
      //获取时间并格式化
      val time_str: String = arr(3).substring(1)
      val time_local: String = format2.format(format1.parse(time_str))
      //浏览器相关信息,如果长度>12则将浏览器信息进行合并
      var http_user_agent: String = arr(11)
      if (arr.length > 12) {
        for (i <- (12 until arr.length)) {
          http_user_agent = http_user_agent + arr(i)
        }
      }
      //状态信息
      val status = arr(8)
      //请求的URL链接
      val request = arr(6)
      //如果状态>400则设置该条数据无效
      if (status.toInt > 400) {
        valid = false
      }
      //如果请求URL不在预定义的List中则数据无效
      if(!list.contains(request)){
        valid = false
      }
      //返回元祖
              //ip   user    时间         URL      状态码   大小    来源                       浏览器
      (valid, arr(0), arr(1), time_local, request, status, arr(9), arr(10).replace("\"", ""), http_user_agent)
    })

    val result = lineRDD.map(x=>x._1+"\001"+x._2+"\001"+x._3+"\001"+x._4+"\001"+x._5+"\001"+x._6+"\001"+x._7+"\001"+x._8+"\001"+x._9)
    //将最终结果保存到文件输出
    result.saveAsTextFile(args(1))

    //关闭SparkContext
    sc.stop()
  }
}
