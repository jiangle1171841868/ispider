package com.air.antispider.stream.common.bean

import scala.beans.BeanProperty

/**
  * 封装kafka接收的原始数据
  */
case class Message(
                    @BeanProperty var time_local: String, // 当前系统时间
                    @BeanProperty var request: String, // 请求URL
                    @BeanProperty var request_method: String, // 请求方式
                    @BeanProperty var content_type: String, // 请求的类型
                    @BeanProperty var request_body: String, // 请求体数据
                    @BeanProperty var http_referer: String, // 来源URL
                    @BeanProperty var remote_addr: String, // 客户端出口IP
                    @BeanProperty var http_user_agent: String, // UA信息,操作系统/浏览器信息
                    @BeanProperty var time_iso8601: String, // 请求的时间
                    @BeanProperty var server_addr: String, // 服务器的IP
                    @BeanProperty var http_cookie: String, // Cookie信息
                    @BeanProperty var active_user_num: Long // 用户最大活跃数
                  )

object Message {

  def str2Message(message: String): Message = {

    // 1. 按照#CS#切分
    val messageArr: Array[String] = message.split("#CS#")

    // 2. 封装Message
    Message(
      messageArr(0),
      messageArr(1),
      messageArr(2),
      messageArr(3),
      messageArr(4),
      messageArr(5),
      messageArr(6),
      messageArr(7),
      messageArr(8),
      messageArr(9),
      messageArr(10),
      messageArr(11).toLong
    )
  }
}