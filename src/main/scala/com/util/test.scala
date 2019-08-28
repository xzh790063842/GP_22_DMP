package com.util

import com.Tags.BusinessTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/24 11:44
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List("116.310003,39.991957")
//    val rdd = sc.makeRDD(list)
//    val bs: RDD[String] = rdd.map(t => {
//      val arr = t.split(",")
//      AmapUtil.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
//    })
//    bs.foreach(println)
//
//    sc.stop()
    val ssc = new SQLContext(sc)
    val df = ssc.read.parquet("output/test1")
    df.map(row => {
      val business = BusinessTag.makeTags(row)
      business

    }).foreach(println)

  }
}
