package com.util

import redis.clients.jedis.JedisPool

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/23 20:12
  */
object RedisUtils {

  private val jedisPool = new JedisPool("hadoop01",6379)

  def setCols(
               key: String,
               fieldValues: Map[String, String]
             ): Unit = {
    import scala.collection.JavaConverters._
    val data = fieldValues.map(element => {
      (element._1.getBytes(), element._2.getBytes())
    }).asJava
    val jedis = jedisPool.getResource
    if (data.size()>0)
      jedis.hmset(key.getBytes(), data)

    jedis.close()
  }

  def getCols(key: String,field: String) :String = {
    val jedis = jedisPool.getResource
    val res = jedis.hget(key, field)
    jedis.close()
    res
  }
}