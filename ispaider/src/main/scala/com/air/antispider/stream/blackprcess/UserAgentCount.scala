package com.air.antispider.stream.blackprcess


import com.air.antispider.stream.`trait`.Processor
import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 单位时间内不同UA出现的次数统计
  */
object UserAgentCount extends Processor {

  override def processData(processedDataRDD: RDD[ProcessedData]):RDD[(String,Long)] = {

    processedDataRDD
      .mapPartitions { iter =>
        iter.map { data =>
          // 1. 获取IP 和 UA
          val ip = data.remoteAddr
          val userAgent = data.httpUserAgent

          // 2. 返回二元组
          ip -> userAgent
        }
      }
      // 3. 聚合 -> 将相同ip携带的UA聚合在迭代器里面
      .aggregateByKey(new mutable.HashSet[String])(

      // a. 分区聚合
      (set: mutable.HashSet[String], ua: String) => {
        // 将ua封装在Set集合中 -> 去重
        set += ua
      },

      // b. 全局聚合
      (set1: mutable.HashSet[String], set2: mutable.HashSet[String]) => {
        set1 ++= set2
      }
    )
      .map { case (ip, set) => (ip, set.size.toLong) }

  }

}
