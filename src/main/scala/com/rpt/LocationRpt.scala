package com.rpt

import com.util.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

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
object LocationRpt {
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
    val listRDD: RDD[((String, String), List[Double])] = df.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //key值 是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      //创建三个对应的方法处理九个指标

      val list1: List[Double] = RptUtils.request(requestmode, processnode)
      val list2 = RptUtils.click(requestmode, iseffective)
      val list3 = RptUtils.ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      ((pro, city), list1 ::: list2 ::: list3)

    })
    val resRDD: RDD[((String, String), List[Double])] = listRDD.reduceByKey((x, y) => (x zip y).map(x => x._1 +x._2))


    val resRow: RDD[Row] = resRDD.map(rdd => {
      val list = rdd._2
      Row(rdd._1._1, rdd._1._2, list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8))
    })

    val schema = StructType(StructField("省份", StringType) :: StructField("市", StringType) ::
      StructField("原始请求", DoubleType) :: StructField("有效请求", DoubleType) :: StructField("广告请求数", DoubleType)
      :: StructField("展示数", DoubleType) :: StructField("点击数", DoubleType)
      :: StructField("参与竞价数", DoubleType) :: StructField("竞价成功数", DoubleType) :: StructField("广告消费", DoubleType) :: StructField("广告成本", DoubleType):: Nil)
    val resDF: DataFrame = sqlContext.createDataFrame(resRow, schema)
    resDF.show()
    /**
      * SparkSQL
      */
//    df.registerTempTable("tmp")
//
//    sqlContext.sql("select\nt.pname,\nt.cname,\nsum(init_request),\nsum(valid_request),\nsum(ad_request),\nsum(join_price),\nsum(success),\nsum(show),\nsum(click),\nsum(DSP_consume)/1000,\nsum(DSP_cost)/1000\nfrom\n(\nselect\nprovincename pname,\ncityname cname,\n(case when requestmode=1 and processnode>=1 then 1 else 0 end) init_request,\n(case when requestmode=1 and processnode>=2 then 1 else 0 end) valid_request,\n(case when requestmode=1 and processnode>=3 then 1 else 0 end) ad_request,\n(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) join_price,\n(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end) success,\n(case when requestmode=2 and iseffective=1 then 1 else 0 end) show,\n(case when requestmode=3 and iseffective=1 then 1 else 0 end) click,\n(case when iseffective=1 and isbilling=1 and iswin=1  then winprice else 0 end) DSP_consume,\n(case when iseffective=1 and isbilling=1 and iswin=1  then adpayment else 0 end) DSP_cost\nfrom tmp\n) t\ngroup by pname,cname")
//      .show()

    sc.stop()
  }
}