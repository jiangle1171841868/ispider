package com.air.antispider.stream.run

import com.air.antispider.stream.blackprcess.{CookieCount, CriticalPagesAccessCount, CriticalPagesAccessIntervalCount, CriticalPagesAccessMinInterval, IPAccessCount, IPBlockAccessCount, TravelTypeCount, UserAgentCount}
import com.air.antispider.stream.common.bean.{ProcessedData, QueryDataPackage, QueryRequestData}
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import com.air.antispider.stream.common.util.kafka.KafkaOffsetUtil
import com.air.antispider.stream.preprocess.refreshBroadcast.RefreshBroadcast
import com.air.antispider.stream.preprocess.rule.AnalyzeRuleDB
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object BlackProcessRun {

  def main(args: Array[String]): Unit = {

    // 一. 构建StreamingContext实例对象
    val ssc =
      StreamingContext.getActiveOrCreate(
        // CHECK_POINT_PATH,
        () => {
          // a. 创建sparkConf对象
          val sparkConf = new SparkConf()
            .setMaster("local[3]")
            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
            // 设置每秒钟每分区的最大读取量
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
          // 设置使用Kryo序列
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          // 注册哪些类型使用Kryo序列化, 注册RDD中类型
          //.registerKryoClasses(Array(classOf[QueryRequestData]))

          // b. 创建SparkContext对象
          val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

          // c. 创建StreamingContext对象
          val streamingContext = new StreamingContext(sc, Seconds(5))

          // d. 设置检查点
          //streamingContext.checkpoint(CHECK_POINT_PATH)

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

  /**
    * 具体处理数据
    *
    * @param ssc
    * @param sc
    */
  def processData(ssc: StreamingContext, sc: SparkContext) = {

    //  从MySQL中获取关键页面
    val criticalPagesRules: ArrayBuffer[String] = AnalyzeRuleDB.queryCriticalPages()
    //  设置广播变量
    var criticalPagesRulesBroadcast: Broadcast[ArrayBuffer[String]] = sc.broadcast(criticalPagesRules)

    //  从MySQL中获取打分规则


    // 1. 从kafka获取数据
    // a. 设置kafka配置信息
    val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> "node01:9092,node02:9092,node03:9092")
    // b. 设置消费的topic
    val topic: String = PropertiesUtil.getStringByKey("source.query.topic", "kafkaConfig.properties")
    val topics: Set[String] = Set(topic)
    var kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    // c. 从zookeeper中获取偏移量数据 -> 获取到就从zk上保存的偏移量开始消费 -> 第一次运行获取不到使用默认的消费方式
    //创建zk客户端
    val zkHosts: String = PropertiesUtil.getStringByKey("zkHosts", "zookeeperConfig.properties")
    val zkPath: String = PropertiesUtil.getStringByKey("rulecompute.antispider.zkPath", "zookeeperConfig.properties")
    val zkClient = new ZkClient(zkHosts)

    //获取Zookeeper上面的kafka偏移量  ->将topic和partition封装到样例类TopicAndPartition中 -> 将TopicAndPartition和偏移量封装到Map集合中
    val topicAndPartitionOpt: Option[Map[TopicAndPartition, Long]] = KafkaOffsetUtil.readOffsets(zkClient, zkHosts, zkPath, topic)

    // 判断是否获取到偏移量
    kafkaDStream = topicAndPartitionOpt match {
      case Some(topicAndPartition) => {
        // zk中有偏移量,从zk保存的偏移量中获取
        /**
          * def createDirectStream[
          *     - K: ClassTag,
          *     - V: ClassTag,
          *     - KD <: Decoder[K]: ClassTag,
          *     - VD <: Decoder[V]: ClassTag,
          *     - R: ClassTag]     //  函数messageHandler返回值的类型 -> Key Value的类型
          * (
          *     - ssc: StreamingContext,
          *     - kafkaParams: Map[String, String],
          *     - fromOffsets: Map[TopicAndPartition, Long],     // 保存在zk上的偏移量 TopicAndPartition-> 是一个样例类封装topic和partition
          *     - messageHandler: MessageAndMetadata[K, V] => R  // 函数 => 返回反序列化后的Key 和 Value
          * ): InputDStream[R]
          */
        val handler = (messageHandler: MessageAndMetadata[String, String]) => (messageHandler.key(), messageHandler.message())

        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, topicAndPartition, handler)
      }
      case None => {
        // zk中没有偏移量,按照默认的方式消费
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      }
    }

    // 保存偏移量到zk
    kafkaDStream.foreachRDD { rdd => KafkaOffsetUtil.saveOffsets(zkClient, zkHosts, zkPath, rdd) }

    // 2. 数据切分 -> 封装成ProcessedData
    // a. 获取DStream中的message
    val inputDStream: DStream[String] = kafkaDStream.transform { rdd => rdd.map { case (key, message) => message } }

    // b. 封装数据
    val sourceDStream: DStream[ProcessedData] = QueryDataPackage.queryDataLoadAndPackage(inputDStream)

    // 设置时间窗口 -> 5秒钟计算前5分钟的数据 -> 必须是批次的整数倍
    sourceDStream.window(Minutes(5), Seconds(5))

    // 3. 数据处理
    sourceDStream.foreachRDD { (rdd, time) =>

      val sc = rdd.sparkContext

      val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)

      println("-------------------------------------------")
      println(s"Time: $batchTime")
      println("-------------------------------------------")

      // 判断RDD是否为空 => 没有数据就不处理
      if (!rdd.isEmpty()) {

        // 每一批次数据执行操作的时候,对广播变量进行更新
        criticalPagesRulesBroadcast = RefreshBroadcast.refreshCriticalPagesRulesBroadcast(sc, criticalPagesRulesBroadcast, "criticalPagesRules")

        // a. 单位时间内的 IP 段访问量（前两位）
        val ipBlockAccessCountMap: collection.Map[String, Long] = IPBlockAccessCount.processData(rdd).collectAsMap()
        ipBlockAccessCountMap.foreach(println)

        // b. 单位时间内的 IP 的访问量
        val ipAccessCountMap: collection.Map[String, Long] = IPAccessCount.processData(rdd).collectAsMap()
        ipAccessCountMap.foreach(println)

        // c. 单位时间内的关键页面访问总量
        val criticalPagesAccessCountMap: collection.Map[String, Long] = CriticalPagesAccessCount.processData(rdd, criticalPagesRulesBroadcast).collectAsMap()
        criticalPagesAccessCountMap.foreach(println)

        // d. 单位时间内的 UA 出现次数统计
        val userAgentCountMap: collection.Map[String, Long] = UserAgentCount.processData(rdd).collectAsMap()
        userAgentCountMap.foreach(println)

        // e. 单位时间内的关键页面最短访问间隔
        val criticalPagesAccessMinIntervalMap: collection.Map[String, Long] = CriticalPagesAccessMinInterval.processData(rdd, criticalPagesRulesBroadcast).collectAsMap()
        criticalPagesAccessMinIntervalMap.foreach(println)

        // f. 单位时间内小于最短访问间隔（预设）的关键页面查询次数
        val criticalPagesAccessIntervalCountMap: collection.Map[String, Long] = CriticalPagesAccessIntervalCount.processData(rdd, criticalPagesRulesBroadcast).collectAsMap()
        criticalPagesAccessIntervalCountMap.foreach(println)

        // g. 单位时间内关键页面的访问次数的 Cookie 数少于 X
        val cookieCountMap: collection.Map[String, Long] = CookieCount.processData(rdd).collectAsMap()
        cookieCountMap.foreach(println)

        // h. 单位时间内查询不同行程的次数
        val travelTypeCountMap: collection.Map[String, Long] = TravelTypeCount.processData(rdd).collectAsMap()
        travelTypeCountMap.foreach(println)

      }

    }

  }
}
