package com.cy.memeber.controller

import com.cy.memeber.bean.DwsMember
import com.cy.memeber.service.DwsMemberService
import com.cy.util.HiveUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-07 18:10
  */
object DwsMemberController {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    //spark.sql.shuffle.partitions 该参数代表了shuffle read task的并行度，该值默认是200，task个数一般是cpu个数的2-3倍
    val sparkConf = new SparkConf().setAppName("dws_member_import").set("spark.sql.shuffle.partitions", "60") .setMaster("local[*]")
          //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化库，如果要使用Java序列化库，需要把该行屏蔽掉
    //在Kryo序列化库中注册自定义的类集合，如果要使用Java序列化库，需要把该行屏蔽掉
          //.registerKryoClasses(Array(classOf[DwsMember]))  //或者conf.set("spark.kryo.registrator", "atguigu.com.MyKryoRegistrator")
      //广播join小表的大小，默认10485760 10M,用api时需要手动广播，比如//broadcast(spark.table("src")).join(spark.table("records"), "key").show()
    //import org.apache.spark.sql.functions.broadcast
      .set("spark.sql.autoBroadcastJoinThreshold","104857600" )
      .set("spark.reducer.maxSizeInFilght", "96mb") //reduce task能够拉取多少数据量的一个参数默认48MB
      .set("spark.shuffle.file.buffer","64k")//每个shuffle文件输出流的内存缓冲区大小，调大此参数可以减少在创建shuffle文件时进行磁盘搜索和系统调用的次数，默认参数为32k 一般调大为64k。

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    //    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    DwsMemberService.importMember(sparkSession,"20190722") //根据用户信息聚合用户表数据
    //DwsMemberService.importMemberUseApi(sparkSession, "20190722") //采用api的方式聚合用户表数据  调优测试

  }
}
