package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 14:46
  */
object TagsChannel extends Tag{
  /**
    * 打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val channel: Int = row.getAs[Int]("adplatformproviderid")

    if(channel != 0){
      list:+=("CN"+channel,1)
    }

    list
  }
}
