package com.rpt

import com.util.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/21 10:28
  */
/**
  * 地域分布指标
  */
object IspRpt {
  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
      //设置序列化方式，采用Kryo序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.parquet(inputPath)

    /**
      * SparkCore
      */
    //将数据进行处理，统计各个指标
    val listRDD: RDD[(String, List[Double])] = df.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")


      val ispname = row.getAs[String]("ispname")

      //创建三个对应的方法处理九个指标

      val list1: List[Double] = RptUtils.request(requestmode, processnode)
      val list2 = RptUtils.click(requestmode, iseffective)
      val list3 = RptUtils.ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      (ispname, list1 ::: list2 ::: list3)

    })
    val resRDD: RDD[(String, List[Double])] = listRDD.reduceByKey((x, y) => (x zip y).map(x => x._1+x._2))



    val resRow: RDD[Row] = resRDD.map(rdd => {
      val list = rdd._2
      Row(rdd._1, list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7)/1000, list(8)/1000)
    })

    val schema = StructType(StructField("运营商", StringType) ::
      StructField("原始请求", DoubleType) :: StructField("有效请求", DoubleType) :: StructField("广告请求数", DoubleType)
      :: StructField("展示数", DoubleType) :: StructField("点击数", DoubleType)
      :: StructField("参与竞价数", DoubleType) :: StructField("竞价成功数", DoubleType) :: StructField("广告消费", DoubleType) :: StructField("广告成本", DoubleType):: Nil)
    val resDF: DataFrame = sqlContext.createDataFrame(resRow, schema)
    resDF.show()

    sc.stop()
  }
}