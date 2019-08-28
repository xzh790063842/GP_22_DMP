package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.{JedisConnectionPool, TagUtils}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    // todo 调用HbaseAPI
    //加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    //创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    configuration.set("hbase.zookeeper.property.clientPort",load.getString("hbase.port"))
    //创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    //创建JobConf
    val jobConf = new JobConf(configuration)
    //指定输出类型和表
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    // 读取数据
    val Array(inputPath,days) = args
    val df = sQLContext.read.parquet(inputPath)
    val stopword = sc.textFile("data/stopwords.txt").map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
        .mapPartitions(row=>{
      val res = row.map(row => {
        // 取出用户Id
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        // val appList = TagsApp.makeTags(row,jedis)
        val appList: List[(String, Int)] = TagsApp.makeTags(row)
        val keywordList = TagKeyWord.makeTags(row, bcstopword)
        val dvList = TagsEuipment.makeTags(row)
        val loactionList = TagsChannel.makeTags(row)
        (userId, adList ++ appList ++ keywordList ++ dvList ++ loactionList)
      })
      res
        })
      .reduceByKey((list1,list2)=> {
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1 ::: list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_ + _._2))
          .toList
      }).map{
      case (userid,userTag) =>{

        val put = new Put(Bytes.toBytes(userid))
        //处理下标签
        val tags = userTag.map(t => t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      //保存到对应表中
      .saveAsHadoopDataset(jobConf)
  }
}