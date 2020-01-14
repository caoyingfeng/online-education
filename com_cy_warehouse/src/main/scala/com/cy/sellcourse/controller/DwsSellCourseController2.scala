package com.cy.sellcourse.controller

import com.cy.sellcourse.service.DwsSellCourseService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-11 11:02
  */
object DwsSellCourseController2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")//广播表大小设为1，即不采用广播表
      //.set("spark.sql.shuffle.partitions", "15") .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    //共执行5.6min 小表join大表1.7min 可以解决数据倾斜的问题但是效率没有提高
    DwsSellCourseService.importSellCourseDetail2(sparkSession,"20190722")
  }
}
