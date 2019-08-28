package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.TagUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
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
object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1){
      println("目录不匹配，退出程序")
      sys.exit()
    }

    var list = List[(String,Int)]()
    val Array(inputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //读取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    val sw: collection.Map[String, Int] = sc.textFile("data/stopwords.txt")
      .map((_,0)).collectAsMap()
    val swbc = sc.broadcast(sw)
    //过滤符合Id的数据
    val baseRDD = df.filter(TagUtils.OneUserId)
      .map(row => {
        val userList: List[String] = TagUtils.getAllUserId(row)
        (userList,row)
      })
    //构建点集合
    val vertexRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      //所有标签
      val adList: List[(String, Int)] = TagsAd.makeTags(row)
      val appList: List[(String, Int)] = TagsApp.makeTags(row)
      val channelList = TagsChannel.makeTags(row)
      val EquipList = TagsEuipment.makeTags(row)
      val keywordList = TagKeyWord.makeTags(row, swbc)
      val businessList = BusinessTag.makeTags(row)
      val AllTag = adList ++ appList ++ channelList ++ EquipList ++ keywordList ++ businessList
      //List((String,Int))
      //保证其中一个点携带者所有标签，同时也保留所有userId
      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ AllTag //((userid,0),(标签),(标签))
      //处理所有的点集合
      tp._1.map(uId => {
        //保证一个点携带标签,(uid,vd),(uid,List()),(uid,List())
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    //vertexRDD.foreach(println)

    //构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode.toLong, uId.hashCode.toLong, 0))
    })
    //edges.take(20).foreach(println)
    //构建图
    val graph: Graph[List[(String, Int)], Int] = Graph(vertexRDD,edges)
    //取出顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //处理所有的标签和id
    vertices.join(vertexRDD).map{
      case (uId,(conId,tagsAll)) => (conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).take(20).foreach(println)

    sc.stop()
  }
}