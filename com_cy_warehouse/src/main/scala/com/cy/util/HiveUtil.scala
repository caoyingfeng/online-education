package com.cy.util

import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-07 18:03
  */
object HiveUtil {
  /**
    * 动态分区有默认大小的设置，需要调大动态分区数，比如在导入3年数据的时候需要调大
    * 调大最大分区数
    * @param spark
    * @return
    */
  def setMaxPartition(spark:SparkSession) ={
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
    spark.sql("set hive.exec.max.created.files=100000")
  }

  /**
    * 开启压缩
    * @param spark
    * @return
    */
  def openCompression(spark:SparkSession)={
    spark.sql("set mapred.output.compress=true")
    spark.sql("set hive.exec.compress.output=true")
  }

  /**
    * 开启动态分区，非严格模式
    *
    * @param spark
    */
  def openDynamicPartition(spark: SparkSession) = {
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  }

  /**
    * 使用lzo压缩
    *
    * @param spark
    */
  def useLzoCompression(spark: SparkSession) = {
    spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
    spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
  }

  /**
    * spark默认使用snappy压缩，不用指定，如果跑hive,需要指定
    * 使用snappy压缩
    * @param spark
    */
  def useSnappyCompression(spark:SparkSession)={
    spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
    spark.sql("set mapreduce.output.fileoutputformat.compress=true")
    spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
  }
}
