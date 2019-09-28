package com.air.antispider.stream.blackprcess

import com.air.antispider.stream.`trait`.Processor
import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.rdd.RDD

/**
  * 统计单位时间内 IP 段的访问量
  */
object IPBlockAccessCount extends Processor {

  override def processData(processedDataRDD: RDD[ProcessedData]): RDD[(String, Long)] = {

    processedDataRDD
      .filter { processData => processData.remoteAddr.split("\\.").length == 4 }
      .mapPartitions { iter =>
        iter.map { processData =>
          // 1. 获取 IP
          val ip: String = processData.remoteAddr
          // 2. 切割 IP , 获取 IP 段, 前两位
          val arr: Array[String] = ip.split("\\.")
          // 3. 拼接获取 IP 段
          val ipBlock = s"${arr(0)}.${arr(1)}"
          // 4. 返回
          ipBlock -> 1L
        }
      }
      // 5. 聚合
      .reduceByKey(_ + _)
  }
}
