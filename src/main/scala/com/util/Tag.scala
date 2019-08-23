package com.util

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 11:15
  */
trait Tag {
  /**
    * 打标签的接口
    */
  def makeTags(args:Any*):List[(String,Int)]
}
