package com.cy.sellcourse.controller

import com.cy.sellcourse.service.DwdSellCourseService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * spark2-submit  --master yarn --deploy-mode client --driver-memory 1g --num-executors 2 --executor-cores 2 --executor-memory 2g -- class com.cy.sellcourse.controller.DwdSellCourseController  --queue spark com_cy_warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar
  *
  * @author cy
  * @create 2020-01-11 10:11
  */
object DwdSellCourseController {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_sellcourse_import")//.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    DwdSellCourseService.importSaleCourseLog(ssc,sparkSession)
    DwdSellCourseService.importCoursePay(ssc,sparkSession)
    DwdSellCourseService.importCourseShoppingCart(ssc,sparkSession)
  }

}
