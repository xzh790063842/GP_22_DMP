package com.util

/**
  * Description:XXX
  *
  * Author:xzh
  *
  * Date:2019/8/21 10:45
  */
/**
  * 指标方法
  */
object RptUtils {

  //此方法处理请求数量
  def request(requestmode:Int,processnode:Int) :List[Double] = {
    var init_request = 0.0
    var valid_request = 0.0
    var ad_request = 0.0
    if (requestmode ==1 && processnode>=1){
      init_request = 1.0
    }
    if (requestmode ==1 && processnode>=2){
      valid_request = 1.0
    }
    if (requestmode ==1 && processnode==3){
      ad_request = 1.0
    }
    List(init_request,valid_request,ad_request)
  }

  //此方法处理展示点击数
  def click(requestmode:Int,iseffective:Int):List[Double] = {
    var show = 0.0
    var click = 0.0
    if (requestmode==2 && iseffective==1){
      show = 1.0
    }
    if (requestmode==3 && iseffective==1){
      click = 1.0
    }
    List(show,click)
  }


  //此方法处理竞价操作
  def ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double] = {
    var join_price = 0.0
    var success = 0.0
    var DSP_consume = 0.0
    var DSP_cost = 0.0
    if (iseffective == 1 && isbilling==1 && isbid == 1){
      join_price = 1.0
    }
    if (iseffective == 1 && isbilling==1 && iswin == 1 && adorderid !=0){
      success = 1.0
    }
    if (iseffective == 1 && isbilling==1 && iswin == 1 ){
      DSP_consume = winprice
    }
    if (iseffective == 1 && isbilling==1 && iswin == 1 ){
      DSP_cost = adpayment
    }

    List(join_price,success,DSP_consume,DSP_cost)
  }


}
