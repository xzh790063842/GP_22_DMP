package com.util



import com.alibaba.fastjson.{JSON, JSONObject}
/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/24 11:19
  */
/**
  * 商圈解析工具
  */
object AmapUtil {

  //获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String = {
    val location = long + "," + lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=b460bf3449daecb37e9af3d9bdac2b11"
    //调用请求
     val jsonstr = HttpUtils.get(urlStr)

    val jsonparse = JSON.parseObject(jsonstr)
    //判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status==0) return ""
    //接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addresComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if (addresComponentJson == null || addresComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addresComponentJson.getJSONArray("businessAreas")
    if (businessAreasArray == null || businessAreasArray.isEmpty) return ""

    //创建集合，保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    //循坏输出
    for(item <- businessAreasArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
