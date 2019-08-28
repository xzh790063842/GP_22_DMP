package com.exam

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer


/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/24 15:15
  */
object ExamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val jsonRDD = sc.textFile("data/exam/json.txt")
    val jsonStr: Array[String] = jsonRDD.collect()
    val jsonArr: Array[JSONObject] = jsonStr.map(json => {
      JSON.parseObject(json)
    })

    val array: Array[ListBuffer[(String, Int)]] = jsonArr.map(jsonparse => {
      //判断状态是否成功
      val status = jsonparse.getIntValue("status")

      if (status == 0) return 0

      val regeocodeJson = jsonparse.getJSONObject("regeocode")
      if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""


      val poiArray = regeocodeJson.getJSONArray("pois")
      if (poiArray == null || poiArray.isEmpty) return ""

      val buffer = collection.mutable.ListBuffer[String]()
      for (item <- poiArray.toArray()) {
        if (item.isInstanceOf[JSONObject]) {
          val json = item.asInstanceOf[JSONObject]
          buffer.append(json.getString("businessarea"))
        }
      }
      val tuples = buffer.map(ele => (ele, 1))
      println(tuples)
      tuples
    })

    val restuples: Array[(String, Int)] = array.map(ele => {
      val array: Array[(String, Int)] = ele.toArray
      (array(0)._1, array.size)
    })

    println(restuples.toBuffer)
  }
}
