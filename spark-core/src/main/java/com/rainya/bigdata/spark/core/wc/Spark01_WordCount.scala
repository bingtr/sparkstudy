package com.rainya.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //建立spark框架连接
    var sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务操作
    //1. 读取文件
    val lines: RDD[String]=sc.textFile("datas\\\\{/*}")

    //2. 扁平化lines操作
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3. 单词分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    //4. 对分组数据进行转换
    val wordToCount = wordGroup.map {
      case (word,list) =>{
        (word,list.size)
      }
    }

    //5. 将转换结果采集到控制台
    val arry: Array[(String, Int)] = wordToCount.collect()
    arry.foreach(println)

    //关闭连接
    sc.stop()
  }
}
