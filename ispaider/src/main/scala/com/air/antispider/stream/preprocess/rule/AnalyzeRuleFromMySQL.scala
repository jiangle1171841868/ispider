package com.air.antispider.stream.preprocess.rule

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.air.antispider.stream.common.bean.AnalyzeRule
import com.air.antispider.stream.common.util.database.{QueryDB, c3p0Util}
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum}

import scala.collection.mutable.ArrayBuffer

/**
  * 从MySQl中获取规则
  */
object AnalyzeRuleFromMySQL {

  /**
    * 获取url过滤规则
    * @return
    */
    def getFilterRuleList(): ArrayBuffer[String]={

    // 查询语句
    val sql = "select value from nh_filter_rule"

    // 查询的字段
    val fileld = "value"

    /// 执行查询
    QueryDB.queryData(sql, fileld)

  }

  /**
    * 获取国内、国际 查询、预订规则
    * @return
    */
  def getClassifyRule():Map[String, ArrayBuffer[String]]={
    val field = "expression"
    //国内查询
    val nationalQuerySQL = "select expression from nh_classify_rule where flight_type = "+ FlightTypeEnum.National.id +" and operation_type = " + BehaviorTypeEnum.Query.id
    val nationalQueryList: ArrayBuffer[String] = QueryDB.queryData(nationalQuerySQL, field)
    //国内预订
    val nationalBookSQL = "select expression from nh_classify_rule where flight_type = "+ FlightTypeEnum.National.id +" and operation_type = " + BehaviorTypeEnum.Book.id
    val nationalBookList: ArrayBuffer[String] = QueryDB.queryData(nationalBookSQL, field)
    //国际查询
    val internationalQuerySQL = "select expression from nh_classify_rule where flight_type = "+ FlightTypeEnum.International.id +" and operation_type = " + BehaviorTypeEnum.Query.id
    val internationalQueryList: ArrayBuffer[String] = QueryDB.queryData(internationalQuerySQL, field)
    //国际预订
    val internationalBookSQL = "select expression from nh_classify_rule where flight_type = "+ FlightTypeEnum.International.id +" and operation_type = " + BehaviorTypeEnum.Book.id
    val internationalBookList: ArrayBuffer[String] = QueryDB.queryData(internationalBookSQL, field)

    //定义一个Map用来封装4个查询结果
    val map = Map(
      "nationalQueryList" -> nationalQueryList,
      "nationalBookList" -> nationalBookList,
      "internationalQueryList" -> internationalQueryList,
      "internationalBookList" -> internationalBookList
    )
    map
  }

  /**
    *
    * 查询 查询 或者 预定 的解析规则 -> 封装到AnalyzeRule里面 -> 封装到List集合 -> 添加到广播变量
    * @param behaviorType : mysql中对应的字段 ->（0-查询，1-预订）
    * @return
    */
  def queryRule(behaviorType: Int): List[AnalyzeRule] = {

    var analyzeRuleList = new ArrayBuffer[AnalyzeRule]()
    val sql: String = "select * from analyzerule where behavior_type =" + behaviorType
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = c3p0Util.getConnection
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val analyzeRule = new AnalyzeRule()
        analyzeRule.id = rs.getString("id")
        analyzeRule.flightType = rs.getString("flight_type").toInt
        analyzeRule.BehaviorType = rs.getString("behavior_type").toInt
        analyzeRule.requestMatchExpression = rs.getString("requestMatchExpression")
        analyzeRule.requestMethod = rs.getString("requestMethod")
        analyzeRule.isNormalGet = rs.getString("isNormalGet").toBoolean
        analyzeRule.isNormalForm = rs.getString("isNormalForm").toBoolean
        analyzeRule.isApplicationJson = rs.getString("isApplicationJson").toBoolean
        analyzeRule.isTextXml = rs.getString("isTextXml").toBoolean
        analyzeRule.isJson = rs.getString("isJson").toBoolean
        analyzeRule.isXML = rs.getString("isXML").toBoolean
        analyzeRule.formDataField = rs.getString("formDataField")
        analyzeRule.book_bookUserId = rs.getString("book_bookUserId")
        analyzeRule.book_bookUnUserId = rs.getString("book_bookUnUserId")
        analyzeRule.book_psgName = rs.getString("book_psgName")
        analyzeRule.book_psgType = rs.getString("book_psgType")
        analyzeRule.book_idType = rs.getString("book_idType")
        analyzeRule.book_idCard = rs.getString("book_idCard")
        analyzeRule.book_contractName = rs.getString("book_contractName")
        analyzeRule.book_contractPhone = rs.getString("book_contractPhone")
        analyzeRule.book_depCity = rs.getString("book_depCity")
        analyzeRule.book_arrCity = rs.getString("book_arrCity")
        analyzeRule.book_flightDate = rs.getString("book_flightDate")
        analyzeRule.book_cabin = rs.getString("book_cabin")
        analyzeRule.book_flightNo = rs.getString("book_flightNo")
        analyzeRule.query_depCity = rs.getString("query_depCity")
        analyzeRule.query_arrCity = rs.getString("query_arrCity")
        analyzeRule.query_flightDate = rs.getString("query_flightDate")
        analyzeRule.query_adultNum = rs.getString("query_adultNum")
        analyzeRule.query_childNum = rs.getString("query_childNum")
        analyzeRule.query_infantNum = rs.getString("query_infantNum")
        analyzeRule.query_country = rs.getString("query_country")
        analyzeRule.query_travelType = rs.getString("query_travelType")
        analyzeRule.book_psgFirName = rs.getString("book_psgFirName")
        analyzeRuleList += analyzeRule
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      c3p0Util.close(conn, ps, rs)
    }
    analyzeRuleList.toList
  }

  /**
    * 查询ip黑名单
    *
    * @return
    */
  def queryBlackIp(): ArrayBuffer[String] = {
    //mysql中ip黑名单数据
    val nibsql = "select ip_name from nh_ip_blacklist"
    val nibField = "ip_name"
    val ipInitList = QueryDB.queryData(nibsql, nibField)
    ipInitList
  }

}
