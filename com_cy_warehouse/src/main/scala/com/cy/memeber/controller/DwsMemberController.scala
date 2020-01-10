package com.cy.memeber.controller

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
    //spark.sql.shuffle.partitions 该参数代表了shuffle read task的并行度，该值默认是200
    val sparkConf = new SparkConf().setAppName("dws_member_import").set("spark.sql.shuffle.partitions", "60") //.setMaster("local[*]")
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用Kryo序列化库，如果要使用Java序列化库，需要把该行屏蔽掉
    //在Kryo序列化库中注册自定义的类集合，如果要使用Java序列化库，需要把该行屏蔽掉
     //     .registerKryoClasses(Array(classOf[DwsMember]))  //或者conf.set("spark.kryo.registrator", "atguigu.com.MyKryoRegistrator")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    ssc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
    HiveUtil.openCompression(sparkSession) //开启压缩
    //    HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩
    DwsMemberService.importMember(sparkSession,"20190722") //根据用户信息聚合用户表数据
    //DwsMemberService.importMemberUseApi(sparkSession, "20190722") //采用api的方式聚合用户表数据

  }
}
