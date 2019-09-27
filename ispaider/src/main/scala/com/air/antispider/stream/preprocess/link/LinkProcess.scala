package com.air.antispider.stream.preprocess.link

import com.air.antispider.stream.`trait`.Processor
import com.air.antispider.stream.common.bean.Message
import com.air.antispider.stream.common.util.jedis.{JedisConnectionUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import redis.clients.jedis.JedisCluster

/**
  * 链路统计
  * 需求:
  *    - 1. 链路访问量      ->   某个IP的累计访问量
  *    - 2. 活跃连接数      ->   取每一个批次数据的最后一条
  * 步骤:
  *    - 1. 获取IP,统计每个IP被访问的次数 =>  封装到Map集合中 key,value => IP,count
  *    - 2. 获取IP和活跃连接数           =>  取每一个批次数据的最后一条
  *    - 3. 将数据封装到Map集合中
  */
object LinkProcess {

  def processData(rdd: RDD[Message]): Map[String, Int] = {

    // 获取jedis连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // 1. 链路访问量  =>  数据的采集量
    val serverCountRDD: RDD[(String, Int)] = rdd
      .mapPartitions { iter =>

        iter.map { message =>
          // a. 获取IP -> 转化为二元组
          val ip: String = message.server_addr
          (ip, 1)
        }
      }
      // b. 对IP分区聚合 -> 求每个链路的访问量
      .reduceByKey { (temp, value) => temp + value }

    // 2. 活跃连接数
    val activeNumRDD: RDD[(String, Long)] = rdd
      .mapPartitions { iter =>
        iter.map { message =>
          // a. 获取IP和活跃连接数
          val ip: String = message.server_addr
          val activeNum: Long = message.active_user_num
          (ip, activeNum)
        }
      }
      // b. 取每一个批次数据的最后一条 -> 临时变量在前面,后面是集合中的元素,最后一条一定在后面 -> 直接返回后面的元素就能够获得迭代器中的最后一个元素
      .reduceByKey { (temp, value) => value }

    // 3. 将数据存在redis中,进行前端展示 -> 前端读取redis数据封装到LinkJsonVO中,数据结构保持一致
    // a. 将RDD转化为Map集合
    val serversCountMap: collection.Map[String, Int] = serverCountRDD.collectAsMap()
    val activeNumMap: collection.Map[String, Long] = activeNumRDD.collectAsMap()

    // b. 将数据封装成Map集合
    val map = Map(
      "serversCountMap" -> serversCountMap,
      "activeNumMap" -> activeNumMap
    )

    // c. 将Map集合转化为json字符串
    val json: String = Json(DefaultFormats).write(map)
    // println(json)

    // 存入Redis的时候,Key必须为CSANTI_MONITOR_LP开头
    // d. 获取存入redis的key -> 前端获取redis数据的时是根据key前缀获取的 -> 由于每批次都有活跃连接数 -> key不能一样,否则就覆盖了,所以加上时间戳
    val key: String = PropertiesUtil.getStringByKey("cluster.key.monitor.linkProcess", "jedisConfig.properties") + System.currentTimeMillis()

    // e. 设置数据的有效期
    val ex: Int = PropertiesUtil.getStringByKey("cluster.exptime.monitor", "jedisConfig.properties").toInt

    // f. 将数据保存到redis
    jedis.setex(key, ex, json)

    // g. 返回serverCountRDD -> Map -> 后面状态监控使用
    serverCountRDD.collectAsMap().toMap
  }
}
