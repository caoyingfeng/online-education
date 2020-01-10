package com.cy.qz.dao

import org.apache.spark.sql.SparkSession

/**
  * @author cy
  * @create 2020-01-08 20:50
  */
object AdsQzDao {

  /**
    * 基于宽表统计各试卷平均耗时、平均分
    * @param sparkSession
    * @param dt
    */
  def getAvgSPendTimeAndScore(sparkSession:SparkSession,dt:String): Unit ={
    sparkSession.sql("select paperviewid, paperviewname, cast(avg(score) as decimal(4,1)) avgscore," +
      " cast(avg(spendtime) as decimal(10,2)) avgspendtime,dt,dn" +
      s"from dws.dws_user_paper_detail where dt='$dt' group by paperviewid,paperviewname,dt,dn order by avgscore desc, avgspendtime desc")
  }

  /**
    * 统计试卷 最高分 最低分
    * @param sparkSession
    * @param dt
    */
  def getTopScore(sparkSession: SparkSession,dt:String): Unit ={
    sparkSession.sql("select paperviewid, paperviewname, cast(max(score) as decimal(4,1)) maxscore, " +
      "cast(min(score) as decimal(4,1)) minscore, dt,dn " +
      "from  dws.dws_user_paper_detail where dt='$dt' group by paperviewid,paperviewname,dt,dn order by maxsore desc")
  }

  /**
    * 按试卷分组统计每份试卷的前三用户详情
    * @param sparkSession
    * @param dt
    */
  def getTop3UserDetail(sparkSession: SparkSession,dt:String): Unit ={
    //有误，应该先select rank 后再进行过滤
    /*sparkSession.sql("select userid,paperviewid, paperviewname,chaptername,pointname,sitecoursename,coursename," +
      "majorname,shortname,sitename,papername,dense_rank() over(partition by paperviewname,dt,dn order by score desc) rank from" +
      "dws.dws_user_paper_detail where dt='$dt' and rank<=3")*/

    sparkSession.sql("select * from(select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename," +
      "coursename,majorname,shortname,sitename,papername,score, dense_rank() over(partition by paperviewid order by score desc) as rk,dt,dn " +
      "from dws.dws_user_paper_detail where dt='$dt' ) " +
      "where rk <4")
  }

  /**
    * 按试卷分组统计每份试卷的倒数前三的用户详情
    * @param sparkSession
    * @param dt
    */
  def getLow3UserDetail(sparkSession: SparkSession,dt:String): Unit ={
    sparkSession.sql("select * from(select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename," +
      "coursename,majorname,shortname,sitename,papername,score, dense_rank() over(partition by paperviewid order by score asc) as rk,dt,dn " +
      "from dws.dws_user_paper_detail where dt='$dt') " +
      "where rk <4")
  }

  /**
    * 统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
    * @param sparkSession
    * @param dt
    */
  def getPaperScoreSegmentUser(sparkSession: SparkSession,dt:String): Unit ={
    /*select
    paperviewid,
    paperviewname,
    (case score when score>=0 and score<20 then '0-20'
    when score>=20 and score<40 then '20-40'
    when score>=40 and score<60 then '40-60'
    when score>=60 and score<80 then '60-80'
    when score>=80 and score<=100 then '80-100' end) score_segment,
    concat_ws(',',collect_set(userid)) userids,
    dt,
    dn
    from dws.dws_user_paper_detail
    where dt = '$dt'
    group by paperviewid,paperviewname,score_segment,userids,dt,dn
    order by paperviewid,score_segment*/

    sparkSession.sql("select paperviewid,paperviewname,score_segment," +
      "concat_ws(',',collect_list(cast(userid as string))) as userids, dt,dn" +
      "from " +
      "(" +
      "   select " +
      "    paperviewid,paperviewname,userid," +
      "    case when score>=0 and score<=20 then '0-20'" +
      "         when score>20 and score<=40 then '20-40'" +
      "         when score>40 and score<=60 then '40-60'" +
      "         when score>60 and score<=80 then '60-80'" +
      "         when score>80 and score<=100 then '80-100' end as score_segment," +
      "    dt,dn" +
      "    from dws.dws_user_paper_detail where dt='$dt'" +
      ")" +
      "group by paperviewid,paperviewname,score_segment,dt,dn order by paperviewid,score_segment")
  }

  /**
    *  统计各试卷未及格人数 及格人数 及格率
    * @param sparkSession
    * @param dt
    */
  def getPaperPassDetail(sparkSession: SparkSession,dt:String): Unit ={

    /*select
    paperviewid,paperviewname,
    count(if score<60,1,0) unpasscount,
    count(if score>60,1,0) passcount,
    cast(passcount/(unpasscount+passcount) as decimal(2,2)) as rate,
    dt,
    dn
    from dws.dws_user_paper_detail where dt='$dt'
    group by paperviewid,paperviewname,dt,dn*/

   /* select t1.paperviewid,t1.paperviewname,cast(passcount/(unpasscount+passcount) as decimal(4,2)) as rate,dt,dn
    from
    (
      (select
        paperviewid,paperviewname,count(*) passcount,dt,dn
        from dws.dws_user_paper_detail where dt='$dt' and scre > 60
    group by paperviewid,paperviewname,dt,dn
    )t1 join
      (select
        paperviewid,paperviewname,count(*) unpasscount,dt,dn
        from dws.dws_user_paper_detail where dt='$dt' and scre between 0 and 60
    group by paperviewid,paperviewname,dt,dn
    )t2 on t1.paperviewid=t2.paperviewid and t1.dn = t2.dn
    )t3*/

    sparkSession.sql("select t3.*,cast(t1.passcount/(t1.passcount+t2.unpasscount) as decimal(4,2)) as rate,dt,dn" +
      "from" +
      "(select t1.paperviewid,t1.paperviewname,t1.unpasscount,t1.dt,t1.dn,t2.passcount" +
      "from" +
      "    (select" +
      "    paperviewid,paperviewname,count(*) unpasscount,dt,dn" +
      "    from dws.dws_user_paper_detail where dt='$dt' and scre between 0 and 60" +
      "    group by paperviewid,paperviewname,dt,dn" +
      "    )t1 join " +
      "    (select" +
      "        paperviewid,count(*) passcount,dn" +
      "    from dws.dws_user_paper_detail where dt='$dt' and scre > 60" +
      "    group by paperviewid,paperviewname,dt,dn" +
      "    )t2 on t1.paperviewid=t2.paperviewid and t1.dn = t2.dn" +
      ")t3")
  }

  /**
    * 统计各题 正确人数 错误人数 错题率 top3错误题数多的questionid
    * @param sparkSession
    * @param dt
    */
  def getQuestionDetail(sparkSession: SparkSession,dt:String): Unit ={
    /*select
    t.*，cast(errcount/(rightcount+errcount) as decimal(4,2)) rate,dt,dn
    from(
      select
        questionid,
      sum(if user_question_answer=1,1,0) rightcount,
      sum(if user_question_answer=0,1,0) errcount,
      dt,
      dn
        from dws.dws_user_paper_detail where dt='$dt'
    group by questionid,dt,dn,user_question_answer
    )t*/

    sparkSession.sql("select t.*,cast(t.errcount/(t.errcount+t.rightcount) as decimal(4,2))as rate" +
      "from(" +
      "    (select questionid,count(*) errcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='0' " +
      "    group by questionid,dt,dn) a join" +
      "    (select questionid,count(*) rightcount,dt,dn from dws.dws_user_paper_detail where dt='$dt' and user_question_answer='1'" +
      "    group by questionid,dt,dn) b " +
      "    on a.questionid=b.questionid and a.dn=b.dn" +
      ")t order by errcount desc")
  }
}
