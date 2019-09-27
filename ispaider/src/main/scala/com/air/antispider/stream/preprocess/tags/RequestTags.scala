package com.air.antispider.stream.preprocess.tags

import java.util.regex.{Matcher, Pattern}

import com.air.antispider.stream.common.bean.RequestType
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum, TravelTypeEnum}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

/**
  * 请求标签标签
  */
object RequestTags {

  // RequestType => 封装请求类型的样例类
  def getRequestType(request: String, broadcast: Broadcast[Map[String, ArrayBuffer[String]]]): RequestType = {

    // 1. 获取广播变量的值
    val map: Map[String, ArrayBuffer[String]] = broadcast.value

    // 2. 获取map集合中的取出四个集合 -> 每一个集合里面封装的是请求类型的url
    val nationalQueryRules: ArrayBuffer[String] = map.getOrElse("nationalQueryList", null)
    val nationalBookRules: ArrayBuffer[String] = map.getOrElse("nationalBookList", null)
    val internationalQueryRules: ArrayBuffer[String] = map.getOrElse("internationalQueryList", null)
    val internationalBookRules: ArrayBuffer[String] = map.getOrElse("internationalBookList", null)

    // 3. 根据集合中的请求类型规则url来匹配url,判断是那种请求类型
    // a. 国内查询
    if (nationalQueryRules != null) {
      // 遍历规则,进行匹配
      for (rule <- nationalQueryRules) {

        if (request.matches(rule)) {
          //如果匹配上.直接返回国内查询
          return RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Query)
        }
      }
    }

    // b. 国内预订
    if (nationalBookRules != null) {
      // 遍历规则,进行匹配
      for (rule <- nationalBookRules) {

        if (request.matches(rule)) {
          //如果匹配上.直接返回国内查询
          return RequestType(FlightTypeEnum.National, BehaviorTypeEnum.Book)
        }
      }
    }

    // c. 国际查询
    if (internationalQueryRules != null) {
      // 遍历规则,进行匹配
      for (rule <- internationalQueryRules) {

        if (request.matches(rule)) {
          //如果匹配上.直接返回国内查询
          return RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Query)
        }
      }
    }

    // d. 国际预订
    if (internationalBookRules != null) {
      // 遍历规则,进行匹配
      for (rule <- internationalBookRules) {

        if (request.matches(rule)) {
          //如果匹配上.直接返回国内查询
          return RequestType(FlightTypeEnum.International, BehaviorTypeEnum.Book)
        }
      }
    }

    //如果上面都没匹配上,那么返回一个默认值other
    RequestType(FlightTypeEnum.Other, BehaviorTypeEnum.Other)
  }

  /**
    * 往返标签
    *
    * @param httpReferrer -> 在ref里面有查询预订时间信息 -> 一个时间是单程 -> 两个时间是往返
    * @return
    */
  def getTravelType(httpReferrer: String) = {

    // 编译一个正则表达式
    val pattern: Pattern = Pattern.compile("(\\d{4})-(0\\d{1}|1[0-2])-(0\\d{1}|[12]\\d{1}|3[01])")

    // 匹配
    val matcher: Matcher = pattern.matcher(httpReferrer)

    var count = 0 // 定义一个变量日期个数

    // 遍历
    while (matcher.find()) {

      count += 1
    }

    // 判断count结果
    if (count == 1) {
      // 单程
      TravelTypeEnum.OneWay
    } else if (count == 2) {
      // 往返
      TravelTypeEnum.RoundTrip
    } else {
      TravelTypeEnum.Unknown
    }

  }

}
