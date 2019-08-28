package com.Tags

import com.util.{RedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 14:10
  */
object TagsApp extends Tag{
  /**
    * 打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    //val row2 = args(1).asInstanceOf[Broadcast[collection.Map[String, String]] ]



    val appType: String = row.getAs[String]("appname")
    val appIdType: String = row.getAs[String]("appid")

//    appType match {
//      case v if StringUtils.isNotBlank(v) => list:+=("APP"+v,1)
//      case _ => list:+=("APP"+row2.value.getOrElse(appIdType,"其它"),1)
//    }

    if (StringUtils.isNotBlank(appType)){
      list:+=("APP"+appType,1)
    }else{
      if (StringUtils.isNotBlank(appIdType)){
        val redisName = RedisUtils.getCols("01",appIdType)
        if (StringUtils.isNotBlank(redisName)){
          list:+=("APP"+redisName,1)
        }else{
          list:+=("APP其他",1)
        }
      }
    }
    list
  }
}
