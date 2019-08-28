package com.proandcount

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/21 9:40
  */
/**
  * 需求指标：统计各省市数据量分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {


    //判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      //设置序列化方式，采用Kryo序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //设置压缩方式，使用snappy方式进行压缩
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val df: DataFrame = sqlContext.read.parquet(inputPath)
    //注册临时表
    df.registerTempTable("log")
    val result = sqlContext.sql("select count(*),provincename,cityname from log group by provincename,cityname")
    //输出到一个文件  coalesce重分区算子
    //result.coalesce(1).write.json(outputPath)

    //加载配置文件，需要使用对应的依赖
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    result.write.mode("append")
      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)


    sc.stop()
  }
}
