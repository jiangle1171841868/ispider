package com.air.antispider.stream.common.bean

import java.util.regex.Pattern

import com.air.antispider.stream.common.util.decode.{EscapeToolBox, RequestDecoder}
import com.air.antispider.stream.common.util.jedis.PropertiesUtil

/**
  * 对数据封装,从message中获取需要的数据
  */
case class DataMessage(
                        request: String, //请求的URL
                        requestMethod: String, //请求方式
                        contentType: String, //请求的类型
                        requestBody: String, //请求体内容:json/xml
                        httpReferrer: String, //请求的来源URL
                        remoteAddr: String, //客户端IP.
                        httpUserAgent: String, //浏览器信息
                        timeIso8601: String, //请求时间
                        serverAddr: String, //服务器IP
                        cookiesStr: String, //原始的Cookie字符串
                        cookieValue_JSESSIONID: String, //从原始Cookie字符串中提取的sessionID
                        cookieValue_USERID: String //从原始Cookie字符串中提取的用户ID
                      )


object DataMessage {

  def message2DataMessage(message: Message): DataMessage = {

    // 获取requset中的url -> //POST /B2C40/dist/main/images/loadingimg.jpg HTTP/1.1
    val request = message.request.split(" ")(1)

    // 获取cookie
    val cookie: String = message.getHttp_cookie

    // 分割cookie获取sessionID和用户ID -> 并保存为 K-V 形式
    val cookieMap = {
      var tempMap = new scala.collection.mutable.HashMap[String, String]
      if (!cookie.equals("")) {
        cookie.split(";").foreach { s =>
          val kv = s.split("=")
          //UTF8 解码
          if (kv.length > 1) {
            try {
              val chPattern = Pattern.compile("u([0-9a-fA-F]{4})")
              val chMatcher = chPattern.matcher(kv(1))
              var isUnicode = false
              while (chMatcher.find()) {
                isUnicode = true
              }
              if (isUnicode) {
                tempMap += (kv(0) -> EscapeToolBox.unescape(kv(1)))
              } else {
                tempMap += (kv(0) -> RequestDecoder.decodePostRequest(kv(1)))
              }
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        }
      }
      tempMap
    }

    val cookieKey_JSESSIONID = PropertiesUtil.getStringByKey("cookie.JSESSIONID.key",
      "cookieConfig.properties")
    val cookieKey_userId4logCookie = PropertiesUtil.getStringByKey("cookie.userId.key",
      "cookieConfig.properties")
    //Cookie-JSESSIONID
    val cookieValue_JSESSIONID = cookieMap.getOrElse(cookieKey_JSESSIONID, "NULL")
    //Cookie-USERID-用户 ID
    val cookieValue_USERID = cookieMap.getOrElse(cookieKey_userId4logCookie, "NULL")


    // 封装数据

    DataMessage(
      request,
      message.request_method,
      message.content_type,
      message.request_body,
      message.http_referer,
      message.remote_addr,
      message.http_user_agent,
      message.time_iso8601,
      message.server_addr,
      cookie,
      cookieValue_JSESSIONID,
      cookieValue_USERID
    )
  }

}