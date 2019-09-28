package com.air.antispider.stream.blackprcess

import com.air.antispider.stream.`trait`.Processor
import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 单位时间内关键页面的访问总量
  */
object CriticalPagesAccessCount {

  def processData(rdd: RDD[ProcessedData], criticalPagesRulesBroadcast: Broadcast[ArrayBuffer[String]]): RDD[(String, Long)] = {

    // 获取广播变量的值
    val criticalPagesRules: ArrayBuffer[String] = criticalPagesRulesBroadcast.value

    rdd
      .mapPartitions { iter =>
        iter.map { data =>
          // 1. 获取IP 和 URL
          val ip: String = data.remoteAddr
          val url: String = data.request
          // 2. 遍历规则   ->  判断是否收集关键页面
          // a. 设置状态值 ->  是关键页面赋值为1  不是关键页面就是默认值0
          var flag = 0L
          for (criticalPages <- criticalPagesRules) {
            if (url.matches(criticalPages)) {
              // a. 是关键页面 -> flag值为1
              flag = 1L
            }
          }
          // 3. 返回二元组
          ip -> flag
        }
      }
      // 4. 聚合
      .reduceByKey(_ + _)
  }

}
