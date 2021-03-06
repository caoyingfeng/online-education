package com.cy.memeber.service

import com.cy.memeber.bean.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import com.cy.memeber.dao.DwdMemberDao
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
  * 1.对dwd层的6张表进行合并，生成一张宽表
  * 2.针对dws层宽表的支付金额（paymoney）和vip等级(vip_level)这两个会变动的字段生成一张拉链表，需要一天进行一次更新
  *以下使用API和spark sql两种方式
  * @author cy
  * @create 2020-01-07 18:24
  */
object DwsMemberService {
  def importMemberUseApi(sparkSession: SparkSession,dt:String): Unit = {
    import sparkSession.implicits._
    //dao层写一些基本sql语句，只是一些基本的单表查询，返回DataFrame
    //todo 1.获取dwd层6张表的基础信息
    val dwdMember = DwdMemberDao.getDwdMember(sparkSession).where(s"dt='${dt}'") //主表用户表
    val dwdMemberRegtype = DwdMemberDao.getDwdMemberRegType(sparkSession)
    val dwdBaseAd = DwdMemberDao.getDwdBaseAd(sparkSession)
    val dwdBaseWebsite = DwdMemberDao.getDwdBaseWebSite(sparkSession)
    val dwdPcentermemPaymoney = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
    val dwdVipLevel = DwdMemberDao.getDwdVipLevel(sparkSession)

    //todo 2. 6张表进行join,方式1 得到的是新的DataFrame,不会将uid去重,两张表的uid都存在
    // 之后调用select算子时就会报错，不知道用哪个uid,字段名不同时可以使用 疑问1
    val frame: DataFrame = dwdMember.join(dwdMemberRegtype,dwdMember("uid") === dwdMemberRegtype("uid") && dwdMember("dn") === dwdMemberRegtype("dn"))
    //方式2 字段名称相等的情况下选用第二种方式更好，得到新的DataFrame,会将uid去重
    //left 与left_outer是一样的
    //将小表进行广播
    import org.apache.spark.sql.functions.broadcast
    val result=dwdMember.join(dwdMemberRegtype, Seq("uid", "dn"), "left")
      .join(broadcast(dwdBaseAd), Seq("ad_id", "dn"), "left_outer")
      .join(broadcast(dwdBaseWebsite), Seq("siteid", "dn"), "left_outer")
      .join(broadcast(dwdPcentermemPaymoney), Seq("uid", "dn"), "left_outer")
      .join(broadcast(dwdVipLevel), Seq("vip_id", "dn"), "left_outer")
      //todo 3.选出与表对应的字段组成case class: DwsMember
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "dt", "dn").as[DwsMember]//.rdd
    //rdd默认缓存在内存，如果内存不够，则一部分会缓存在内存中，剩下的会从血缘依赖处再去计算
    //spark内存分为两部分：1.用于shuffle 计算 join groupby 2.storage存储数据
    //spark1.6为静态分配 两部分呢内存比值固定，之后为动态分配
    //不采用序列化时，rdd storage占用的内存为1.7G
    //result.cache()

    //采用kryo序列化后，rdd storage占用内存	269.6 MB，采用kryo序列化后，缓存级别需要加上SER
   // result.persist(StorageLevel.MEMORY_ONLY_SER)

    //采用DataSet默认的序列化方式，默认的缓存级别为 MEMORY_AND_DISK ，rdd storage占用内存29.3 MB
    //result.cache()
    //DataSet更改缓存级别 rdd storage占用内存28.8M
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //todo 4.按照 相同用户相同网站 聚合在一起,最终形成宽表
    //疑问2
    val resultData: Dataset[DwsMember_Result] = result.groupByKey(item => item.uid + "_" + item.dn)
      .mapGroups { case (key, iters) =>
        val keys = key.split("_")
        val uid = Integer.parseInt(keys(0))
        val dn = keys(1)
        //iters用到多次，转换成list
        val dwsMembers = iters.toList
        val paymoney = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
        val ad_id = dwsMembers.map(_.ad_id).head
        val fullname = dwsMembers.map(_.fullname).head
        val icounurl = dwsMembers.map(_.iconurl).head
        val lastlogin = dwsMembers.map(_.lastlogin).head
        val mailaddr = dwsMembers.map(_.mailaddr).head
        val memberlevel = dwsMembers.map(_.memberlevel).head
        val password = dwsMembers.map(_.password).head
        val phone = dwsMembers.map(_.phone).head
        val qq = dwsMembers.map(_.qq).head
        val register = dwsMembers.map(_.register).head
        val regupdatetime = dwsMembers.map(_.regupdatetime).head
        val unitname = dwsMembers.map(_.unitname).head
        val userip = dwsMembers.map(_.userip).head
        val zipcode = dwsMembers.map(_.zipcode).head
        val appkey = dwsMembers.map(_.appkey).head
        val appregurl = dwsMembers.map(_.appregurl).head
        val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
        val reg_createtime = dwsMembers.map(_.reg_createtime).head
        val isranreg = dwsMembers.map(_.isranreg).head
        val regsource = dwsMembers.map(_.regsource).head
        val regsourcename = dwsMembers.map(_.regsourcename).head
        val adname = dwsMembers.map(_.adname).head
        val siteid = dwsMembers.map(_.siteid).head
        val sitename = dwsMembers.map(_.sitename).head
        val siteurl = dwsMembers.map(_.siteurl).head
        val site_delete = dwsMembers.map(_.site_delete).head
        val site_createtime = dwsMembers.map(_.site_createtime).head
        val site_creator = dwsMembers.map(_.site_creator).head
        val vip_id = dwsMembers.map(_.vip_id).head
        val vip_level = dwsMembers.map(_.vip_level).max
        val vip_start_time = dwsMembers.map(_.vip_start_time).min
        val vip_end_time = dwsMembers.map(_.vip_end_time).max
        val vip_last_modify_time = dwsMembers.map(_.vip_last_modify_time).max
        val vip_max_free = dwsMembers.map(_.vip_max_free).head
        val vip_min_free = dwsMembers.map(_.vip_min_free).head
        val vip_next_level = dwsMembers.map(_.vip_next_level).head
        val vip_operator = dwsMembers.map(_.vip_operator).head
        DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
          phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
          bdp_uuid, reg_createtime, isranreg, regsource, regsourcename, adname, siteid,
          sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
          vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
          vip_next_level, vip_operator, dt, dn)
      }
    resultData.show()
    //result.foreach(println)
    while (true) {
      println("1")
    }
  }

  def importMember(sparkSession: SparkSession, time: String): Unit = {
    import sparkSession.implicits._
    //todo 查询全量数据 刷新到宽表  dn 网站分区  dt日期分区
    //sum(cast(paymoney as decimal(10,4))) 对支付金额进行求和，将金额由String转化为decimal,长度为10精度为4
    //first 为spark中特有的函数
    //group by uid,dn,dt   对paymoney进行了求和,其余内容相同的字段只需取第一个
    sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
      "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
      "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
      "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime)," +
      "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
      "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
      "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
      "first(vip_operator),dt,dn " +
      "from" +
      "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
      "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
      "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.isranreg,b.regsource," +
      "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
      "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
      "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
      "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
      s"from" +
      s" dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " + //以用户表为主表,join 注册表 广告表 网站基础表 用户支付金额表 用户vip等级基础表
      "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
      " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
      s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
      "group by uid,dn,dt").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")

    //todo 针对dws层宽表的支付金额（paymoney）和vip等级(vip_level)这两个会变动的字段生成一张拉链表，需要一天进行一次更新
    //todo 1.查询当天增量数据  得到当天用户的支付金额和vip等级 转成一个dataSet, MemberZipper
    val dayResult: Dataset[MemberZipper] = sparkSession.sql(
      s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
      s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.dn) as dn " +
      " from " +
      "dwd.dwd_pcentermempaymoney a join " +
      s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid").as[MemberZipper]

    //todo 2.查询历史拉链表数据 并转成dataSet
    val historyResult: Dataset[MemberZipper] = sparkSession.sql("select * from dws.dws_member_zipper").as[MemberZipper]

    //todo 3.两份数据根据用户id进行聚合 对end_time和payment进行重新修改
    //groupByKey是Dataset中的,Dataset中没有reduceByKey，
    // groupByKey自定义key, 按照 相同用户相同网站 聚合在一起，groupByKey返回值是KeyValueGroupedDataset,不是Dataset
    //mapGroups将一批数据聚合成一条
    //val MemberZipperGroupByKey: KeyValueGroupedDataset[String, MemberZipper] = dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
    val MemberZipperResultList: Dataset[MemberZipperResult] = dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
      .mapGroups { case (key, iters) =>
        val keys = key.split("_")
        val uid = keys(0)
        val dn = keys(1)
        //iterator只能迭代一次，如果多次使用，转换成list或array数组
        val list = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
        if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
          //如果存在历史数据 需要对历史数据的end_time进行修改
          //获取历史数据的最后一条数据
          val oldLastModel: MemberZipper = list(list.size - 2)
          //获取当前时间最后一条数据
          val lastModel: MemberZipper = list(list.size - 1)
          //更新历史数据
          oldLastModel.end_time = lastModel.start_time
          //更新最新的支付金额
          lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal.apply(oldLastModel.paymoney)).toString()
        }
        MemberZipperResult(list)
      }
    //todo 4.重组对象打散 刷新拉链表
    //拉链表是需要修改表的，hive在0.14版本后支持修改，但是要求表 1) 内部表 2）分桶表  ,但是内部表安全性低
    //此处使用Overwrite覆盖
    MemberZipperResultList.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper")

  }
}
