package com.cy.sellcourse.service

import java.sql.Timestamp

import com.cy.sellcourse.bean.{DwdCourseShoppingCart, DwdSaleCourse}
import com.cy.sellcourse.dao.DwdSellCourseDao
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * dwd.dwd_sale_course 与dwd.dwd_course_shopping_cart  join条件：courseid、dn、dt
  * dwd.dwd_course_shopping_cart 与dwd.dwd_course_pay  join条件：orderid、dn、dt
  * 不允许丢数据，关联不上的字段为null,join之后导入dws层的表
  *
  * 1000门课，3000万加入购物车，2000万支付
  *
  * @author cy
  * @create 2020-01-11 11:20
  */
object DwsSellCourseService {

  def importSellCourseDetail(sparkSession: SparkSession, dt: String): Unit = {
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")

    dwdSaleCourse.join(dwdCourseShoppingCart, Seq("courseid", "dt", "dn"), "right")
      .join(dwdCoursePay, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")

  }

  def importSellCourseDetail2(sparkSession: SparkSession, dt: String): Unit = {
    //解决数据倾斜问题，将小表进行扩容，大表key加上随机散列值
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    //分区设为1
    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime").coalesce(1)

    import sparkSession.implicits._
    //大表的key,拼接随机后缀,
    val newDwdCourseShoppingCart = dwdCourseShoppingCart.mapPartitions(partitions => {
      partitions.map(item => {
        //获取DataFrame的值，两种方式
        // item 类型为row,对应DataSet 的item 为case class或基本类型
        //row类型获取里面的值
        item.getString(0) //方式1.获取表里第一个字段
        val courseid = item.getAs[Int]("courseid")//方式2.通过getAs[]()
        val randInt = Random.nextInt(100) //0-99
        DwdCourseShoppingCart(courseid, item.getAs[String]("orderid"),
          item.getAs[String]("coursename"), item.getAs[java.math.BigDecimal]("cart_discount"),
          item.getAs[java.math.BigDecimal]("sellmoney"), item.getAs[Timestamp]("cart_createtime"),
          item.getAs[String]("dt"), item.getAs[String]("dn"), courseid + "_" + randInt)  //增加随机列,将每一数据根据key进行打散
      })
    })

    //小表进行扩容 对比 宽表 将相同key的数据聚合到一起，再封装到list里，通过flatMap扁平化
    val newDwdSaleCourse = dwdSaleCourse.flatMap(item => {
      val list = new ArrayBuffer[DwdSaleCourse]()
      val courseid = item.getAs[Int]("courseid")
      val coursename = item.getAs[String]("coursename")
      val status = item.getAs[String]("status")
      val pointlistid = item.getAs[Int]("pointlistid")
      val majorid = item.getAs[Int]("majorid")
      val chapterid = item.getAs[Int]("chapterid")
      val chaptername = item.getAs[String]("chaptername")
      val edusubjectid = item.getAs[Int]("edusubjectid")
      val edusubjectname = item.getAs[String]("edusubjectname")
      val teacherid = item.getAs[Int]("teacherid")
      val teachername = item.getAs[String]("teachername")
      val coursemanager = item.getAs[String]("coursemanager")
      val money = item.getAs[java.math.BigDecimal]("money")
      val dt = item.getAs[String]("dt")
      val dn = item.getAs[String]("dn")
      for (i <- 0 until 100) {  //每条数据扩容100倍
        list.append(DwdSaleCourse(courseid, coursename, status, pointlistid, majorid, chapterid, chaptername, edusubjectid,
          edusubjectname, teacherid, teachername, coursemanager, money, dt, dn, courseid + "_" + i))
      }
      list.iterator
    })

    newDwdSaleCourse.join(newDwdCourseShoppingCart.drop("courseid").drop("coursename"),
      Seq("rand_courseid","dt","dn"),"right")
      .join(dwdCoursePay,Seq("orderid","dt","dn"),"left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  def importSellCourseDetail3(sparkSession: SparkSession,dt:String): Unit ={
    //解决数据倾斜的问题，采用广播小表
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")

    import org.apache.spark.sql.functions._
    broadcast(dwdSaleCourse).join(dwdCourseShoppingCart,Seq("courseid", "dt", "dn"),"right")
      .join(dwdCoursePay,Seq("orderid", "dt", "dn"),"left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }

  //分桶join使用条件：两表join的bucket数相等  bucket列=join列=sort列  必须应用在bucket mapjoin 建表时必须是clustered 且sorted
  def importSellCourseDetail4(sparkSession: SparkSession,dt:String): Unit ={
    //解决数据倾斜的问题，采用广播小表
    val dwdSaleCourse = DwdSellCourseDao.getDwdSaleCourse(sparkSession).where(s"dt=${dt}")
    val dwdCourseShoppingCart = DwdSellCourseDao.getDwdCourseShoppingCart(sparkSession).where(s"dt=${dt}")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    val dwdCoursePay = DwdSellCourseDao.getDwdCoursePay(sparkSession).where(s"dt=${dt}")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")

    import org.apache.spark.sql.functions._
    val tempData = dwdCourseShoppingCart.join(dwdCoursePay,Seq("orderid"),"left")

    broadcast(dwdSaleCourse).join(tempData,Seq("courseid"),"right")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dwd.dwd_sale_course.dt", "dwd.dwd_sale_course.dn")
      .write.mode(SaveMode.Overwrite).insertInto("dws.dws_salecourse_detail")
  }


}
