package com.air.antispider.stream.common.bean

import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import com.air.antispider.stream.dataprocess.constants.{BehaviorTypeEnum, FlightTypeEnum, TravelTypeEnum}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * 将kafka中获取的数据 -> 封装成样例类
  */
object QueryDataPackage {

  def queryDataLoadAndPackage(kafkaDStream: DStream[String]): DStream[ProcessedData] = {

    //使用mapPartitions减少包装类的创建开销
    kafkaDStream
      .mapPartitions { partitionsIterator =>
        //创建json解析
        val mapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        //将数据进行map，一条条处理
        partitionsIterator.map { sourceLine =>
          //分割数据
          val dataArray = sourceLine.split("#CS#", -1)
          //原始数据，站位，并无数据
          val sourceData = dataArray(0)
          val requestMethod = dataArray(1)
          val request = dataArray(2)
          val remoteAddr = dataArray(3)
          val httpUserAgent = dataArray(4)
          val timeIso8601 = dataArray(5)
          val serverAddr = dataArray(6)
          val highFrqIPGroup: Boolean = dataArray(7).equalsIgnoreCase("true")
          val requestType: RequestType = RequestType(FlightTypeEnum.withName(dataArray(8)), BehaviorTypeEnum.withName(dataArray(9)))
          val travelType: TravelTypeEnum = TravelTypeEnum.withName(dataArray(10))
          val requestParams: CoreRequestParams = CoreRequestParams(dataArray(11), dataArray(12), dataArray(13))
          val cookieValue_JSESSIONID: String = dataArray(14)
          val cookieValue_USERID: String = dataArray(15)
          //分析查询请求的时候不需要book数据
          val bookRequestData: Option[BookRequestData] = None
          //封装query数据
          val queryRequestData = if (!dataArray(16).equalsIgnoreCase("NULL")) {
            mapper.readValue(dataArray(16), classOf[QueryRequestData]) match {
              case value if value != null => Some(value)
              case _ => None
            }
          } else {
            None
          }
          val httpReferrer = dataArray(18)

          //封装流程数据，返回
          ProcessedData("", requestMethod, request,
            remoteAddr, httpUserAgent, timeIso8601,
            serverAddr, highFrqIPGroup,
            requestType, travelType, requestParams,
            cookieValue_JSESSIONID, cookieValue_USERID,
            queryRequestData, bookRequestData, httpReferrer)
        }

      }
  }
}