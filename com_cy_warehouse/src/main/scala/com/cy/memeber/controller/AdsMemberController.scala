package com.cy.memeber.controller

import com.cy.memeber.service.AdsMemberService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * 使用Spark DataFrame Api统计通过各注册跳转地址(appregurl)进行注册的用户数
  * 使用Spark DataFrame Api统计各所属网站（sitename）的用户数
  * 使用Spark DataFrame Api统计各所属平台的（regsourcename）用户数
  * 使用Spark DataFrame Api统计通过各广告跳转（adname）的用户数
  * 使用Spark DataFrame Api统计各用户级别（memberlevel）的用户数
  * 使用Spark DataFrame Api统计各vip等级人数
  * 使用Spark DataFrame Api统计各分区网站、用户级别下(website、memberlevel)的top3用户
  *
  * @author cy
  * @create 2020-01-07 21:01
  */
object AdsMemberController {
  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkConf = new SparkConf().setAppName("ads_member_controller")//.setMaster("local[*]")
    //.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区

    AdsMemberService.queryDetailApi(sparkSession, "20190722")
    AdsMemberService.queryDetailSql(sparkSession, "20190722")
  }
}
