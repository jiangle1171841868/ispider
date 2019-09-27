package com.air.antispider.stream.preprocess.senddatatokafka

import java.util
import java.util.concurrent.atomic.AtomicInteger

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD

/**
  * 发送数据到kafka -> 根据操作类型BehaviorTypeEnum分别过滤出查询和预订的数据 -> 发送到不同的topic
  */
object DataSendToKafka {

  def sendDataToKafka(processedDataRDD: RDD[ProcessedData]) = {

    // 将查询和预订发送到不同的topic  变化的是 -> topic 和 数据 -> 根据操作类型来判断

    // 1. 查询类数据 -> 查询在BehaviorTypeEnum中为0
    sendData(processedDataRDD, 0)

    // 2. 预订类数据 -> 查询在BehaviorTypeEnum中为1
    sendData(processedDataRDD, 1)

  }

  /**
    * 具体发送数据的方法
    *
    * @param processedDataRDD
    * @param behaviorTypeId
    */
  def sendData(processedDataRDD: RDD[ProcessedData], behaviorTypeId: Int) = {

    // 1. 根据传入的操作类型数据过滤出发送的数据 ->  behaviorTypeId=0 查询类  behaviorTypeId=1 预订类 -> 过滤出数据,发送到Kafka
    val data = processedDataRDD.filter { processedData => processedData.requestType.behaviorType.id == behaviorTypeId } // 枚举通过字符id获取值

    // 2. 根据传入的操作类型数据决定发送的topic
    var topic = ""
    //查询数据的 topic：target.query.topic = processedQuery
    val queryTopic = PropertiesUtil.getStringByKey("target.query.topic", "kafkaConfig.properties")
    val bookTopic = PropertiesUtil.getStringByKey("target.book.topic", "kafkaConfig.properties")
    //判断本次发送的Topic
    if (behaviorTypeId == 0) {
      topic = queryTopic
    } else if (behaviorTypeId == 1) {
      topic = bookTopic
    }

    // kafka生产者配置信息
    val map = new util.HashMap[String, Object]()
    // kafka集群地址
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtil.getStringByKey("default.brokers", "kafkaConfig.properties"))
    // kafka key序列化
    map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.key_serializer_class_config", "kafkaConfig.properties"))
    // kafka value序列化
    map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PropertiesUtil.getStringByKey("default.value_serializer_class_config", "kafkaConfig.properties"))
    // 批量发送 数据大小作为一批次 或 10ms 作为一批次, 两个条件, 如果有任何一个达到, 都会发送.
    map.put(ProducerConfig.BATCH_SIZE_CONFIG, PropertiesUtil.getStringByKey("default.batch_size_config", "kafkaConfig.properties"))
    map.put(ProducerConfig.LINGER_MS_CONFIG, PropertiesUtil.getStringByKey("default.linger_ms_config", "kafkaConfig.properties"))

    // 遍历RDD
    processedDataRDD.foreachPartition { iter =>

      // a. 创建kafka生产者对象 -> 一个分区创建一个对象
      val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](map)

      iter.foreach { processedData =>

        // b. 将case class 转换为字符串
        val message = processedData.toKafkaString()

        // 发送数据
        kafkaProducer.send(new ProducerRecord(topic, message))
      }
      //关闭连接
      kafkaProducer.close()
    }
  }

}
