package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 14:59
  */
object TagsEuipment extends Tag {
  /**
    * 打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val client = row.getAs[Int]("client")
    val networkmannername = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")


    client match {
      case v if v == 1 => list :+= (v + "Android D00010001", 1)
      case v if v == 2 => list :+= (v + "IOS D00010002", 1)
      case v if v == 3 => list :+= (v + "WinPhone D00010001", 1)
      case _ => list :+= ("D00010001", 1)
    }
    networkmannername match {
      case v if v.equals("WIFI") => list :+= (v + "D00020001",1)
      case v if v.equals("4G") => list :+= (v + "D00020002",1)
      case v if v.equals("3G") => list :+= (v + "D00020003",1)
      case v if v.equals("2G") => list :+= (v + "D00020004",1)
      case _ => list :+= ("D00020005",1)
    }
    ispname match {
      case v if v.equals("移动") => list :+= (v + "D00030001",1)
      case v if v.equals("联通") => list :+= (v + "D00030002",1)
      case v if v.equals("电信") => list :+= (v + "D00030003",1)
      case _ => list :+= ("D00030004",1)
    }


    list
  }
}
