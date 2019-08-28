package com.util

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.spark.SparkConf


/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/24 21:07
  */
object HbaseUtils {

  def main(args: Array[String]): Unit = {
    write2Hbase()
  }
  def write2Hbase() = {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val conf = HBaseConfiguration.create
    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    //Connection是操作hbase的入口
    val connection = ConnectionFactory.createConnection(conf)

    val admin = connection.getAdmin
    val tableName: TableName = TableName.valueOf("hbaseTest")
    if (!admin.tableExists(tableName)){

      //val htd = HTableDescriptor
      //创建表
     // admin.createTable(htd)
      println("create done.")
    }else{
      print("表已存在")
    }

  }
}
