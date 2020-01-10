package com.cy.qz.service

import com.cy.qz.dao._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author cy
  * @create 2020-01-08 17:13
  */
object DwsQzService {
  //维度退化
  //聚合章节维度表
  def saveDwsQzChapter(sparkSession: SparkSession,dt:String): Unit ={
    //todo 1.获取dwd层QzChapter的基础信息
    val dwdQzChapter = QzChapterDao.getDwdQzChapter(sparkSession,dt)
    val dwdQzChapterList = QzChapterDao.getDwdQzChapterList(sparkSession,dt)
    val dwdQzPoint = QzChapterDao.getDwdQzPoint(sparkSession,dt)
    val dwdQzPointQuestion = QzChapterDao.getDwdQzPointQuestion(sparkSession,dt)
    //todo 2. 4张表进行join
    val result: DataFrame = dwdQzChapter.join(dwdQzChapterList, Seq("chapterlistid", "dn"), "inner")
      .join(dwdQzPoint, Seq("chapterid", "dn"), "inner")
      .join(dwdQzPointQuestion, Seq("pointid", "dn"), "inner")
    //todo 3.选出与表对应的字段组成插入表中
    result.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "showstatus",
      "chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
      "pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
      "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn"
    ).coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")
  }

  //聚合课程维度表
  def saveDwsQzCourse(sparkSession: SparkSession,dt:String): Unit ={
    //todo 1.获取dwd层QzCourse的基础信息
    val dwdQzSiteCourse = QzCourseDao.getDwdQzSiteCourse(sparkSession,dt)
    val dwdQzCourse = QzCourseDao.getDwdQzCourse(sparkSession,dt)
    val dwdQzCourseEduSubject = QzCourseDao.getDwdQzCourseEduSubject(sparkSession,dt)
    val result: DataFrame = dwdQzSiteCourse.join(dwdQzCourse, Seq("courseid", "dn"))
      .join(dwdQzCourseEduSubject, Seq("courseid", "dn"))
    result.select("sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter",
      "sequence", "status", "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid",
      "showstatus", "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid"
      , "dt", "dn").coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")
  }

  //聚合主修维度表
  def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
    val dwdQzMajor = QzMajorDao.getQzMajor(sparkSession, dt)
    val dwdQzWebsite = QzMajorDao.getQzWebsite(sparkSession, dt)
    val dwdQzBusiness = QzMajorDao.getQzBusiness(sparkSession, dt)
    val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
      .join(dwdQzBusiness, Seq("businessid", "dn"))
      .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
        "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
        "multicastgateway", "multicastport", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")
  }

  //聚合试卷维度表
  def saveDwsQzPaper(sparkSession: SparkSession,dt:String): Unit ={
    val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(sparkSession,dt)
    val dwdQzCenter = QzPaperDao.getDwdQzCenter(sparkSession,dt)
    val dwdQzPaper = QzPaperDao.getDwdQzPaper(sparkSession,dt)
    val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(sparkSession,dt)

    val result: DataFrame = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
      .join(dwdQzCenter, Seq("centerid", "dn"), "left")
      .join(dwdQzPaper, Seq("paperid", "dn"))
      .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
        , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
        "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
        "paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn"
      )
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")
  }
  //聚合题目维度表
  def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String) = {
    val dwdQzQuestion = QzQuestionDao.getQzQuestion(sparkSession, dt)
    val dwdQzQuestionType = QzQuestionDao.getQzQuestionType(sparkSession, dt)
    val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
      .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
        , "limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
        , "attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
        "remark", "splitscoretype", "dt", "dn")
    result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")
  }

  //基于dws.dws_qz_chapter、dws.dws_qz_course、dws.dws_qz_major、dws.dws_qz_paper、dws.dws_qz_question、
  // dwd.dwd_qz_member_paper_question 合成宽表dw.user_paper_detail,使用spark sql和dataframe api操作
  def saveDwsUserPaperDetail(sparkSession: SparkSession,dt:String): Unit ={
    val dwsQzChapter = UserPaperDetailDao.getDwsQzChapter(sparkSession,dt)
    val dwsQzCourse = UserPaperDetailDao.getDwsQzCourse(sparkSession,dt)
    val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession,dt)
    val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession,dt)
    val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession,dt)
    val dwdQzMemberPaperQuestion = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession,dt)

    dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
      join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
      .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
      .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
        "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
        "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
        , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
        "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
        , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
        "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
        "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
        "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
        "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
        "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
        "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
        "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
        "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
        "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
        "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
        "question_splitscoretype", "user_question_answer", "dt", "dn").coalesce(1)
      .write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")
  }


}
