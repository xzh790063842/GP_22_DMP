package com.Tags

import com.util.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 10:18
  */
/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    val app_dict: RDD[Array[String]] = sc.textFile("data/app_dict.txt").map(_.split("\t")).filter(_.length>=6)
    val idnameRDD: RDD[(String, String)] = app_dict.map(row => {

      val appname = row(1)
      val appid = row(4)
      (appid, appname)
    })
    val map: collection.Map[String, String] = idnameRDD.collectAsMap()

    val broadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(map)

    val sw: collection.Map[String, Int] = sc.textFile("data/stopwords.txt").map((_,0)).collectAsMap()
    val swbc = sc.broadcast(sw)
    //过滤符合Id的数据
    val res: RDD[(String, List[(String, Int)])] = df.filter(TagUtils.OneUserId)
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户Id
      val userId: String = TagUtils.getOneUserId(row)
      //接下来通过row数据 打上所有标签（按照需求）
      //val adList: List[(String, Int)] = TagsAd.makeTags(row)
      //val adList = TagsApp.makeTags(row,broadcast)
      //val adList = TagsChannel.makeTags(row)
      //val adList = TagsEuipment.makeTags(row)
      val adList = TagKeyWord.makeTags(row,swbc)

      (userId, adList)
    })
    println(res.collect.toBuffer)
  }
}
