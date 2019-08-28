package com.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 20:15
  */
object TagsAppUtils {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val app_dict: RDD[Array[String]] = sc.textFile("data/app_dict.txt").map(_.split("\t")).filter(_.length>=6)
    val idnameRDD: RDD[(String, String)] = app_dict.map(row => {
      val appname = row(1)
      val appid = row(4)
      (appid, appname)
    })
    val map: Map[String, String] = idnameRDD.collect.toMap

    RedisUtils.setCols("01",map)

    sc.stop()

  }
}
