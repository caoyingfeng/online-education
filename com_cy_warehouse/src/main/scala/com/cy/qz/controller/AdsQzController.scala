package com.cy.qz.controller

import com.cy.qz.service.AdsQzService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-08 20:30
  */
object AdsQzController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ads_qz_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    //HiveUtil.openCompression(sparkSession) 不能开启压缩
    //HiveUtil.useSnappyCompression(sparkSession)

    val dt = "20190722"
    //AdsQzService.getTarget(sparkSession, dt)
    AdsQzService.getTargetApi(sparkSession,dt)
  }
}
