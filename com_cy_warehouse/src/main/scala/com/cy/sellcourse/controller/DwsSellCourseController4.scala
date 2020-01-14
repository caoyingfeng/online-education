package com.cy.sellcourse.controller

import com.cy.sellcourse.service.DwsSellCourseService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-11 11:02
  */
object DwsSellCourseController4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dws_sellcourse_import")
      .set("spark.sql.autoBroadcastJoinThreshold", "1")//广播表大小设为1，即不采用广播表
      .set("spark.sql.shuffle.partitions", "12") //.setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession)
    HiveUtil.openCompression(sparkSession)

    //小表join大表采用广播join， 大表join大表采用smb join, 通过分桶表实现，一般用于TB级别数据,因为分桶表会产生大量小文件，数据量大了就不是小文件了
    DwsSellCourseService.importSellCourseDetail4(sparkSession,"20190722")
  }
}
