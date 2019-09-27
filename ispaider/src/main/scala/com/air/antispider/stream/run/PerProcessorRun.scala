package com.air.antispider.stream.run

import com.air.antispider.stream.common.bean.{AnalyzeRule, BookRequestData, DataMessage, Message, ProcessedData, QueryRequestData, RequestType}
import com.air.antispider.stream.dataprocess.constants.TravelTypeEnum.TravelTypeEnum
import com.air.antispider.stream.preprocess.encrypted.EncryptedData
import com.air.antispider.stream.preprocess.filter.URLFilter
import com.air.antispider.stream.preprocess.link.LinkProcess
import com.air.antispider.stream.preprocess.monitor.SparkStreamingMonitor
import com.air.antispider.stream.preprocess.refreshBroadcast.RefreshBroadcast
import com.air.antispider.stream.preprocess.rule.{AnalyzeBookRequest, AnalyzeRequest, AnalyzeRuleDB}
import com.air.antispider.stream.preprocess.senddatatokafka.DataSendToKafka
import com.air.antispider.stream.preprocess.tags.{BlackIPTags, RequestTags}
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ArrayBuffer

object PerProcessorRun {


  val CHECK_POINT_PATH: String = "data/spark-streaming/checkpoint/test-001" + System.currentTimeMillis()

  def main(args: Array[String]): Unit = {

    // 一. 构建StreamingContext实例对象
    val ssc =
      StreamingContext.getActiveOrCreate(
        CHECK_POINT_PATH,
        () => {
          // a. 创建sparkConf对象
          val sparkConf = new SparkConf()
            .setMaster("local[3]")
            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
            // 设置每秒钟每分区的最大读取量
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")

          // b. 创建SparkContext对象
          val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

          // c. 创建StreamingContext对象
          val streamingContext = new StreamingContext(sc, Seconds(3))

          // d. 设置检查点
          streamingContext.checkpoint(CHECK_POINT_PATH)

          // 二. 调用方法处理数据
          processData(streamingContext, sc)

          // e. 返回
          streamingContext
        }
      )

    // 三. 启动流式应用
    ssc.start()
    ssc.awaitTermination()

    // 四. 停止应用
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }

  /// TODO: 抽取函数 传递ssc处理对象处理数据

  def processData(ssc: StreamingContext, sc: SparkContext) = {

    //  从MySQL中获取url过滤规则
    val urlFilterRules: ArrayBuffer[String] = AnalyzeRuleDB.getFilterRuleList()
    // 设置广播变量 -> volatile 注解可以安全修改广播变量
    @volatile var urlFilterRulesBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(urlFilterRules)

    // 从MySQL中获取请求规则
    val requestRulesMap: Map[String, ArrayBuffer[String]] = AnalyzeRuleDB.getClassifyRule()
    // 设置广播变量
    @volatile var requestRulesBroadcast: Broadcast[Map[String, ArrayBuffer[String]]] = sc.broadcast(requestRulesMap)

    // 从MySQL中获取解析规则 -> 查询类数据
    val queryRules: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(0)
    // 设置广播变量
    @volatile var queryRulesBroadcast: Broadcast[List[AnalyzeRule]] = sc.broadcast(queryRules)

    // 从MySQL中获取解析规则 -> 预定类数据
    val bookRules: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(1)
    // 设置广播变量
    @volatile var bookRulesBroadcast: Broadcast[List[AnalyzeRule]] = sc.broadcast(bookRules)

    // 从MySQL中获取黑名单IP数据
    val blackIPRules: ArrayBuffer[String] = AnalyzeRuleDB.queryBlackIp()

    // 设置广播变量
    @volatile var blackIPRulesBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(blackIPRules)

    // 1. 从kafka获取数据
    // a. 设置kafka配置信息
    val kafkaParams: Map[String, String] = Map(
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "auto.offset.reset" -> "largest" // 设置获取最新偏移量 -> 默认就是最新
    )
    // b. 设置消费的topic
    val topics: Set[String] = Set("collect_log")

    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    // 从kafkaDStream中获取message (key,message) => message
    val messageDStrame: DStream[String] = kafkaDStream.map { case (key, message) => message }

    // 2. 处理数据
    messageDStrame
      .foreachRDD { (rdd, time) =>

        val sc = rdd.sparkContext

        val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)

        println("-------------------------------------------")
        println(s"Time: $batchTime")
        println("-------------------------------------------")

        // 判断RDD是否为空 => 没有数据就不处理
        if (!rdd.isEmpty()) {

          // 每一批次数据执行操作的时候,对广播变量进行更新
          // 更新url过滤的广播变量
          urlFilterRulesBroadcast = RefreshBroadcast.refreshBroadcast(sc, urlFilterRulesBroadcast, "urlFilterRulesStatus")

          // 更新请求类型的广播变量
          requestRulesBroadcast = RefreshBroadcast.refreshRequestRuleBroadcast(sc, requestRulesBroadcast, " classifyRuleStatus")

          // 查询类数据解析的广播变量
          queryRulesBroadcast = RefreshBroadcast.refreshQueryRulesBroadcast(sc, queryRulesBroadcast, "queryRuleStatus")

          // 预订类数据解析的广播变量
          bookRulesBroadcast = RefreshBroadcast.refreshQueryRulesBroadcast(sc, bookRulesBroadcast, "bookRuleStatus")

          // 黑名单ip的广播变量
          blackIPRulesBroadcast = RefreshBroadcast.refreshBlackIPRulesBroadcast(sc, blackIPRulesBroadcast, "blackRuleStatus")

          // 一. 过滤数据
          val filterRDD: RDD[String] = rdd.filter { message => message.trim.split("#CS#").length >= 12 }

          // 二. 将数据封装成样例类Message,方便数据处理
          val messageRDD: RDD[Message] = filterRDD.map { message => Message.str2Message(message) }


          // 三. 业务处理
          // a. 链路统计 -> 发送到redis -> 供前端展示
          val serverCountMap:Map[String, Int] = LinkProcess.processData(messageRDD)

          // b. URL过滤
          val urlFilterRDD: RDD[Message] = messageRDD.filter { message => URLFilter.urlFilter(message, urlFilterRulesBroadcast) }

          //urlFilterRDD.foreach(println)

          // 处理数据转化为结构化数据
          val processedDataRDD: RDD[ProcessedData] = urlFilterRDD.mapPartitions { iter =>
            iter.map { message =>

              // c. 数据加密
              //   加密手机号
              val encryptedPhoneMessage: Message = EncryptedData.encryptedPhone(message)
              //   加密身份证号
              val encryptedMessage: Message = EncryptedData.encryptedID(encryptedPhoneMessage)

              // d. 数据切割转化 =>  Message -> DataMessage
              val dataMessage: DataMessage = DataMessage.message2DataMessage(encryptedMessage)

              // e. 数据打标签  => 获取url 进行分析
              //    i. 请求类型标签
              val requestType: RequestType = RequestTags.getRequestType(dataMessage.request, requestRulesBroadcast)
              //    ii.单程、往返标签
              val travelType: TravelTypeEnum = RequestTags.getTravelType(dataMessage.httpReferrer)

              // f. 数据解析
              //    i. 查询类数据
              val parseQueryData: Option[QueryRequestData] = AnalyzeRequest.analyzeQueryRequest(
                requestType,
                dataMessage.requestMethod,
                dataMessage.contentType,
                dataMessage.request,
                dataMessage.requestBody,
                travelType,
                queryRulesBroadcast.value
              )

              //    ii.预订类数据
              val parseBookData: Option[BookRequestData] = AnalyzeBookRequest.analyzeBookRequest(
                requestType,
                dataMessage.requestMethod,
                dataMessage.contentType,
                dataMessage.request,
                dataMessage.requestBody,
                travelType,
                bookRulesBroadcast.value
              )

              // g. 数据再加工 -> 黑名单IP打标签
              val blackStatus: Boolean = BlackIPTags.getBlackIPStatus(dataMessage.remoteAddr, blackIPRulesBroadcast)

              // h. 将数据封装到ProcessedData中返回
              val processedData: ProcessedData = ProcessedData.processData(dataMessage, requestType, travelType, parseQueryData, parseBookData, blackStatus)

              processedData

            }
          }

          // i. 发送消息到kafka -> 根据查询和预订将数据发送到不同的topic
          DataSendToKafka.sendDataToKafka(processedDataRDD)

          // j. 监控数据
          SparkStreamingMonitor.streamMonitor(processedDataRDD, sc,serverCountMap)


          processedDataRDD.foreach(println)

        }
      }
  }


}
