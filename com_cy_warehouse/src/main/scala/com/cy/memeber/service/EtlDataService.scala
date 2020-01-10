package com.cy.memeber.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cy.util.ParseJsonData
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}



/**
  * @author cy
  * @create 2020-01-07 16:21
  */
object EtlDataService {
  /**
    * 导入网站表基础数据
    * @param sc
    * @param sparkSession
    */
  def etlBaseWebSiteLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换 rdd可以使用toDF
    sc.textFile("/user/atguigu/ods/baswewebsite.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(log => {
      log.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val siteid = jsonObject.getIntValue("siteid")
        val sitename = jsonObject.getString("sitename")
        val siteurl = jsonObject.getString("siteurl")
        val delete = jsonObject.getIntValue("delete")
        val createtime = jsonObject.getString("createtime")
        val creator = jsonObject.getString("creator")
        val dn = jsonObject.getString("dn")
        (siteid, sitename, siteurl, delete, createtime, creator, dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

  /**
    *用户基本信息表
    * @param sc
    * @param sparkSession
    */
  def etlMemberLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    sc.textFile("/user/atguigu/ods/member.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(memberlog=>{
      memberlog.map(item=>{
        val jsonObject = ParseJsonData.getJsonData(item)
        val ad_id = jsonObject.getIntValue("ad_id")
        val birthday = jsonObject.getString("birthday")
        val email = jsonObject.getString("email")
        val fullname = jsonObject.getString("fullname").substring(0,1) + "xx"
        val iconurl = jsonObject.getString("iconurl")
        val lastlogin = jsonObject.getString("lastlogin")
        val mailaddr = jsonObject.getString("mailaddr")
        val memberlevel = jsonObject.getString("memberlevel")
        val password = "******"
        val paymoney = jsonObject.getString("paymoney")
        val phone = jsonObject.getString("phone")
        val newphone = phone.substring(0,3) + "*****" + phone.substring(7,11)
        val qq = jsonObject.getString("qq")
        val register = jsonObject.getString("register")
        val regupdatetime = jsonObject.getString("regupdatetime")
        val uid = jsonObject.getString("uid")
        val unitname = jsonObject.getString("unitname")
        val userip = jsonObject.getString("userip")
        val zipcode = jsonObject.getString("zipcode")
        val dn = jsonObject.getString("dn")
        val dt = jsonObject.getString("dt")
        (uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr,
          memberlevel, password, paymoney, newphone, qq,
          register, regupdatetime, unitname, userip, zipcode, dt, dn)
      })
    }).toDF.coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
  }

  /**
    * 用户跳转地址注册表
    * @param sc
    * @param sparkSession
    */
  def etlMemberRegtypeLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    sc.textFile("/user/atguigu/ods/memberRegtype.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(regtypelog=>{
      regtypelog.map(item=>{
        val jsonObject = ParseJsonData.getJsonData(item)
        val appkey = jsonObject.getString("appkey")
        val appregurl = jsonObject.getString("appregurl")
        val bdp_uuid = jsonObject.getString("bdp_uuid")
        val createtime = jsonObject.getString("createtime")
        val dn = jsonObject.getString("dn")
        val domain = jsonObject.getString("domain")
        val dt = jsonObject.getString("dt")
        val isranreg = jsonObject.getString("isranreg")
        val regsource = jsonObject.getString("regsource")
        val regsourcename = regsource match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "App"
          case "4" => "WeChat"
          case _ => "other"
        }
        val uid = jsonObject.getIntValue("uid")
        val websiteid = jsonObject.getIntValue("websiteid")
        (uid,appkey,appregurl,bdp_uuid,createtime,isranreg,regsource,regsourcename,websiteid,dt,dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
  }

  /**
    * 广告基础表原始json数据
    * @param sc
    * @param sparkSession
    */
  def etlBaseAdLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    sc.textFile("/user/atguigu/ods/baseadlog.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(baseadlog=>{
      baseadlog.map(item=>{
        val jsonObject = ParseJsonData.getJsonData(item)
        val adname = jsonObject.getString("adname")
        val adid = jsonObject.getIntValue("adid")
        val dn = jsonObject.getString("dn")
        (adid,adname,dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  /**
    * 导入用户付款信息
    * @param sc
    * @param sparkSession
    */
  def etlMemPayMoneyLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    sc.textFile("/user/atguigu/ods/pcentermempaymoney.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(log=>{
      log.map(item=>{
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = jsonObject.getIntValue("uid")
        val siteid = jsonObject.getIntValue("siteid")
        val vip_id = jsonObject.getIntValue("vip_id")
        val paymoney = jsonObject.getString("paymoney")
        val dt = jsonObject.getString("dt")
        val dn = jsonObject.getString("dn")
        (uid,paymoney,siteid,vip_id,dt,dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
  }

  /**
    * 导入用户vip基础数据
    * @param sc
    * @param sparkSession
    */
  def etlMemVipLevelLog(sc:SparkContext,sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._
    sc.textFile("/user/atguigu/ods/pcenterMemViplevel.log")
      .filter(item=>{  //转换成对象，先进行过滤，转换后是否是JSONObject
        val obj = ParseJsonData.getJsonData(item)
        obj.isInstanceOf[JSONObject]
      }).mapPartitions(log=>{
      log.map(item=>{
        val jsonObject = ParseJsonData.getJsonData(item)
        val discountval = jsonObject.getString("discountval")
        val vip_id = jsonObject.getIntValue("vip_id")
        val vip_level = jsonObject.getString("vip_level")
        val start_time = jsonObject.getString("start_time")
        val end_time = jsonObject.getString("end_time")
        val last_modify_time = jsonObject.getString("last_modify_time")
        val max_free = jsonObject.getString("max_free")
        val min_free = jsonObject.getString("min_free")
        val next_level = jsonObject.getString("next_level")
        val operator = jsonObject.getString("operator")
        val dn = jsonObject.getString("dn")
        (vip_id,vip_level,start_time,end_time,last_modify_time,max_free,min_free,next_level,operator,dn)
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }
}
