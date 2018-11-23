package cn.bigdata.weblog

import java.text.SimpleDateFormat
import java.util.{Locale, UUID}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

object ClickStreamPageView {

  def main(args: Array[String]): Unit = {
    //1. 获取SparkContext对象
    val sparkConf: SparkConf = new SparkConf().setAppName("ClickStreamPageView")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //2. 读取文件
    val file: RDD[String] = sc.textFile(args(0))
    val lineRDD: RDD[Array[String]] = file.map(_.split("\001"))

    //=============================================================
    //以IP为key分组, 将value按照时间排序, 为每条记录添加一个session
    val clickStreamPageView: RDD[ListBuffer[Array[String]]] = lineRDD.filter(_(0).toBoolean == true).map(x => (x(1), x)).groupByKey().map(xx => {
      val resultList = ListBuffer[Array[String]]()
      val lineList: Iterable[Array[String]] = xx._2
      //按照时间进行排序
      val sortList: List[Array[String]] = lineList.toList.sortBy(_(4))
      //获取到session
      var session: String = UUID.randomUUID().toString
      //如果该IP只有一条记录, 则直接返回
      if (sortList.size == 1) {
        val list1: Array[String] = sortList(0)
        val strings: Array[String] = Array[String](session, xx._1, list1(2), list1(3), list1(4), list1(5), list1(6), list1(7), list1(8))
        resultList += strings
      } else {
        //如果该IP大于1条记录
        for (i <- (0 until sortList.size)) {
          breakable {
            if (i == 0) {
              break()
            } else {
              var format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
              val timeDiff: Long = format2.parse(sortList(i)(3)).getTime - format2.parse(sortList(i - 1)(3)).getTime
              if (timeDiff < 1000 * 60 * 30) {
                //相隔小于30分钟
                val list1: Array[String] = sortList(i-1)
                val strings: Array[String] = Array[String](session, xx._1, list1(2), list1(3), list1(4), list1(5), list1(6), list1(7), list1(8))
                resultList += strings
              } else {
                //相隔大于30分钟
                val list1: Array[String] = sortList(i-1)
                val strings: Array[String] = Array[String](session, xx._1, list1(2), list1(3), list1(4), list1(5), list1(6), list1(7), list1(8))
                resultList += strings
                session = UUID.randomUUID().toString
              }
            }
          }
          //如果当前是最后一条
          if (i == sortList.size) {
            val list1: Array[String] = sortList(i)
            val strings: Array[String] = Array[String](session, xx._1, list1(2), list1(3), list1(4), list1(5), list1(6), list1(7), list1(8))
            resultList += strings
          }
        }
      }
      resultList
    })

    val map: RDD[String] = clickStreamPageView.map(_.flatten.mkString("\001"))
    map.saveAsTextFile(args(1))

    sc.stop()
  }
}
