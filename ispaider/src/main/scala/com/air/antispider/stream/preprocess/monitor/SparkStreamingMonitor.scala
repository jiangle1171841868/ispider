package com.air.antispider.stream.preprocess.monitor

import java.lang

import com.air.antispider.stream.common.bean.ProcessedData
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import com.air.antispider.stream.common.util.spark.SparkMetricsUtils
import com.alibaba.fastjson.{JSON, JSONObject}
import com.fasterxml.jackson.databind.ser.std.StdKeySerializers.Default
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/**
  * 数据处理监控
  */
object SparkStreamingMonitor {

  def streamMonitor(processedDataRDD: RDD[ProcessedData], sc: SparkContext, serverCountMap: Map[String, Int]) = {

    // 1. 确定访问的URL
    // i. 动态获取节点的主机地址,拼接URL
    //val sparkDriverHost = sc.getConf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES")
    // ii. 拼接URL
    val url = "http://localhost:4040/metrics/json/"

    // 2. 发送http请求获取json数据,解析数据
    val json: JSONObject = SparkMetricsUtils.getMetricsJson(url)

    // a. 获取gauges字段中的数据 -> 里面的数据是对象 -> 使用getJSONObject -> 如果是数组使用getJSONArray
    val gaugesJSON: JSONObject = json.getJSONObject("gauges")

    // b. 获取开始时间和结束时间的数据  -> 里面是对象 -> 获取对象里面的value字段可以获取数据
    //    i. 字段中appID和appName是变化的 -> 需要通过sc动态获取 -> 拼接开始时间和结束时间的key
    val appID = sc.applicationId
    val appName = sc.appName

    val startTimeKey = s"$appID.driver.$appName.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime"
    val endTimeKey = s"$appID.driver.$appName.StreamingMetrics.streaming.lastCompletedBatch_processingEndTime"

    //    ii. 获取时间
    val startTime: Long = gaugesJSON.getJSONObject(startTimeKey).getLong("value").toLong
    val endTime: Long = gaugesJSON.getJSONObject(endTimeKey).getLong("value").toLong

    // c. 获取批次处理时间
    val costTime: Long = endTime - startTime

    // d. 获取批次处理数据量
    val datacount: Long = processedDataRDD.count()

    // e. 获取每毫秒批次处理数据量
    //    为了防止时间异常为0 需要非0校验
    var countPerMillis: Double = 0.0
    if (costTime > 0) {
      // 转化为Double进行计算 -> 可以获得小数 -> 计算准确
      countPerMillis = datacount / costTime.toDouble
    }

    val endTimeStr = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(endTime)

    // 3. 将数据保存到redis中, 供前端展示
    // a. 前端从redis中获取数据封装到JsonVO中 -> 将数据按照JsonVO的字段封装成Map集合(key不用封装) -> 转化为字符串保存到redis中
    val map = Map(
      "costTime" -> costTime,
      "serversCountMap" -> serverCountMap,
      "applicationId" -> appID,
      "applicationUniqueName" -> appName,
      "countPerMillis" -> countPerMillis,
      "endTime" -> endTimeStr,
      "sourceCount" -> datacount
    )

    // b. 将数据转化为字符串 使用json4s转化复杂嵌套的Map集合
    val jonStr: String = Json(DefaultFormats).write(map)

    // c. 保存数据到redis
    //    i. 获取连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    //    ii. 设置保key和保存时间
    val key: String = PropertiesUtil.getStringByKey("cluster.key.monitor.dataProcess", "jedisConfig.properties") + System.currentTimeMillis()
    val ex: Int = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

    // iii. 保存数据
    jedis.setex(key, ex, jonStr)
  }


}
