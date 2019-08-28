package com.util

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils


/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/24 11:14
  */
/**
  * http请求协议
  */
object HttpUtils {

  //GET请求
  def get(url : String):String = {
    val client = HttpClients.createDefault()

    val get = new HttpGet(url)
    //发送请求
    val response: CloseableHttpResponse = client.execute(get)

    //获取返回结果
    val res = EntityUtils.toString(response.getEntity,"UTF-8")
    println(res)
    res
  }
}
