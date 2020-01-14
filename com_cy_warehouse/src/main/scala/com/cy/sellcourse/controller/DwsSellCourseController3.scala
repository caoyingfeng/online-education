package com.cy.sellcourse.controller

import com.cy.sellcourse.service.DwsSellCourseService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-11 11:02
  */
object DwsSellCourseController3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")//广播表大小设为1，即不采用广播表
      .set("spark.sql.shuffle.partitions", "12") //.setMaster("local[*]")   //提交任务时设置了2个work ,每个work2个cpu,总task个数是cpu数的3倍
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    //广播小表 3.4min
    DwsSellCourseService.importSellCourseDetail3(sparkSession,"20190722")
  }
}
