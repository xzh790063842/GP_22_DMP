package com.ETL

import java.util.Properties

import com.util.{SchemaUtils, Utils2Type}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, GroupedData, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/20 10:43
  */
/**
  * 格式转换
  */
object txt2Parquet {
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
    //进行数据得读取，处理分析数据
    val lines: RDD[String] = sc.textFile(inputPath)
    val rowRDD: RDD[Row] = lines.map(t => t.split(",", t.length)).filter(_.length >= 85)
      .map(arr => {
        Row(
          arr(0),
          Utils2Type.toInt(arr(1)),
          Utils2Type.toInt(arr(2)),
          Utils2Type.toInt(arr(3)),
          Utils2Type.toInt(arr(4)),
          arr(5),
          arr(6),
          Utils2Type.toInt(arr(7)),
          Utils2Type.toInt(arr(8)),
          Utils2Type.toDouble(arr(9)),
          Utils2Type.toDouble(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          Utils2Type.toInt(arr(17)),
          arr(18),
          arr(19),
          Utils2Type.toInt(arr(20)),
          Utils2Type.toInt(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          Utils2Type.toInt(arr(26)),
          arr(27),
          Utils2Type.toInt(arr(28)),
          arr(29),
          Utils2Type.toInt(arr(30)),
          Utils2Type.toInt(arr(31)),
          Utils2Type.toInt(arr(32)),
          arr(33),
          Utils2Type.toInt(arr(34)),
          Utils2Type.toInt(arr(35)),
          Utils2Type.toInt(arr(36)),
          arr(37),
          Utils2Type.toInt(arr(38)),
          Utils2Type.toInt(arr(39)),
          Utils2Type.toDouble(arr(40)),
          Utils2Type.toDouble(arr(41)),
          Utils2Type.toInt(arr(42)),
          arr(43),
          Utils2Type.toDouble(arr(44)),
          Utils2Type.toDouble(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          Utils2Type.toInt(arr(57)),
          Utils2Type.toDouble(arr(58)),
          Utils2Type.toInt(arr(59)),
          Utils2Type.toInt(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          Utils2Type.toInt(arr(73)),
          Utils2Type.toDouble(arr(74)),
          Utils2Type.toDouble(arr(75)),
          Utils2Type.toDouble(arr(76)),
          Utils2Type.toDouble(arr(77)),
          Utils2Type.toDouble(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          Utils2Type.toInt(arr(84))
        )
      })
    //按要求切割，并保证数据得长度大于等于85个字段,
    //如果切割得时候遇到相同切割条件重复得情况下，需要切割得话，那么后面需要加上对应匹配参数
    //这样切割才会准确，比如 ,,,,,,会当成一个字符切割，需要加上对应得匹配参数


    //构建DF
    val df: DataFrame = sqlContext.createDataFrame(rowRDD,SchemaUtils.structtype)


    df.registerTempTable("tmp")

    val frame: DataFrame = sqlContext.sql("select count(1) ct,provincename,cityname from tmp group by provincename,cityname")

    //frame.show()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://localhost:3306/gp22dmp?useUnicode=true&characterEncoding=utf-8"
    frame.write.mode(SaveMode.Append).jdbc(url, "prodatacount", prop)
    frame.write.partitionBy("provincename","cityname").save("hdfs://hadoop01:8020/gp22/DMP/out-20190821-1")

    sc.stop()
  }
}