package com.cy.sellcourse.controller

import com.cy.sellcourse.service.DwsSellCourseService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * spark2-submit  --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 2g --class com.cy.sellcourse.controller.DwsSellCourseController  --queue spark com_cy_warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar
  *
  *
  * @author cy
  * @create 2020-01-11 11:02
  */
object DwsSellCourseController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")//不采用广播表
      //.set("spark.sql.shuffle.partitions", "15") .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    //共执行4.3min 小表join大表 1.2min
    DwsSellCourseService.importSellCourseDetail(sparkSession,"20190722")
  }
}
