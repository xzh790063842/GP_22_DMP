package com.Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 16:46
  */
object TagKeyWord extends Tag{
  /**
    * 打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String,Int]]]
    val kwds: Array[String] = row.getAs[String]("keywords").split("\\|")
    kwds.filter(word => {
      word.length >= 3 && word.length<=8 && !stopword.value.contains(word)
    }).foreach(word => list:+=("K"+word,1))

    list
  }
}
