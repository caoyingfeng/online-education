package com.cy.qz.service

import com.cy.qz.dao.AdsQzDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author cy
  * @create 2020-01-08 20:43
  */
object AdsQzservice {
  def queryDetailSql(sparkSession:SparkSession,dt:String): Unit ={
    val avgDetail = AdsQzDao.getAvgSPendTimeAndScore(sparkSession, dt)
    val topscore = AdsQzDao.getTopScore(sparkSession, dt)
    val top3UserDetail = AdsQzDao.getTop3UserDetail(sparkSession, dt)
    val low3UserDetail = AdsQzDao.getLow3UserDetail(sparkSession, dt)
    val paperScore = AdsQzDao.getPaperScoreSegmentUser(sparkSession, dt)
    val paperPassDetail = AdsQzDao.getPaperPassDetail(sparkSession, dt)
    val questionDetail = AdsQzDao.getQuestionDetail(sparkSession, dt)
  }

  def getTargetApi(sparkSession: SparkSession,dt:String): Unit ={
    import org.apache.spark.sql.functions._
    //基于宽表统计各试卷平均耗时、平均分
    sparkSession.sql("select paperviewid, paperviewname,score,spendtime,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").groupBy("paperviewid","paperviewname","dt","dn")
      .agg(avg("score").cast("decimal(4,1)").as("avgscore"),
        avg("spendtime").cast("decimal(4,1)").as("avgspendtime"))
      .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")

    //统计各试卷最高分、最低分
    sparkSession.sql("select paperviewid, paperviewname,score,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}").groupBy("paperviewid","paperviewname","dt","dn")
      .agg(max("score").as("maxscore"),
        min("score").as("minscore"))
      .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")

    //按试卷分组统计每份试卷的前三用户详情
    sparkSession.sql("select userid,paperviewid, paperviewname,chaptername,pointname," +
      "sitecoursename,coursename,majorname,shortname,papername,score,dt,dn from dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .withColumn("rank",dense_rank().over(Window.partitionBy("paperviewid","dn").orderBy(desc("score"))))
      .where("rank<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")

    sparkSession.sql("select * from dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
        , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk",dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")

    //按试卷分组统计每份试卷的倒数前三的用户详情
    sparkSession.sql("select * from dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
        , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
      .withColumn("rk",dense_rank().over(Window.partitionBy("paperviewid").orderBy(asc("score"))))
      .where("rk<4")
      .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
        , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")

    //统计各试卷各分段的用户id
    sparkSession.sql("select * from dws.dws_user_paper_detail")
      .where(s"dt=${dt}")
      .select("paperviewid","paperviewname","score","userid","dt","dn")
      .withColumn("score_segment",
        when(col("score").between(0, 20), "0-20")
        .when(col("score") > 20 && col("score") <= 40, "20-40")
        .when(col("score") > 40 && col("score") <= 60, "40-60")
        .when(col("score") > 60 && col("score") <= 80, "60-80")
        .when(col("score") > 80 && col("score") <= 100, "80-100")
      )
      .drop("score").groupBy("paperviewid","paperviewname","score_segment","dt","dn")
      .agg(concat_ws(",",collect_list(col("userid").cast("string"))).as("userids"))
      .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .orderBy("paperviewid","score_segment")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")

    //统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
    //缓存
    val paperPassDetatil: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()
    val unPassDetail = paperPassDetatil.select("paperviewid", "paperviewname", "score", "dt", "dn")
      .where(s"dt=${dt}").where("score between 0 and 60").drop("score")
      .groupBy("paperviewid", "paperviewname", "dt", "dn")
      .agg(count("paperviewid").as("unpasscount"))

    //val passDetail = paperPassDetatil.select("paperviewid", "paperviewname", "score", "dt", "dn")
    val passDetail = paperPassDetatil.select("paperviewid",  "dn","score")
      .where(s"dt=${dt}").where("score > 60").drop("score")
      .groupBy("paperviewid", "dn")
      .agg(count("paperviewid")).as("passcount")

    unPassDetail.join(passDetail,Seq("paperviewid", "dn"))
      .withColumn("rate",
        (col("passcount") / (col("passcount") + col("unpasscount"))).cast("decimal(4,2)"))
      .select("paperviewid", "paperviewname","passcount", "unpasscount","rate", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")
    //释放缓存
    paperPassDetatil.unpersist()

    //统计各题的错误数，正确数，错题率
    val userQuestionDetail: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()
    val userQuestionError = userQuestionDetail.select("questionid", "dt", "dn","user_question_answer")
      .where(s"dt=${dt}").where("user_question_answer='0'").drop("user_question_answer")
      .groupBy("questionid", "dn","dt")
      .agg(count("questionid").as("errcount"))

    val userQuestionRight = userQuestionDetail.select("questionid", "dn","user_question_answer")
      .where(s"dt=${dt}").where("user_question_answer='1'").drop("user_question_answer")
      .groupBy("questionid", "dn")
      .agg(count("questionid").as("rightcount"))

    userQuestionError.join(userQuestionRight,Seq("questionid","dn"))
      .withColumn("rate",
        (col("errcount")/(col("errcount")+col("rightcount"))).cast("decimal(4,2)"))
      .orderBy("errcount")
      .select("questionid", "errcount","rightcount","rate","dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")
    userQuestionDetail.unpersist()
  }

}
