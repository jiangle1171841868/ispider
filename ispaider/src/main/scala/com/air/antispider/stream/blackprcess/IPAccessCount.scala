package com.air.antispider.stream.blackprcess

import com.air.antispider.stream.`trait`.Processor
import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.spark.rdd.RDD

/**
  * 单位时间内 IP 的访问量
  */
object IPAccessCount extends Processor {

  override def processData(processedDataRDD: RDD[ProcessedData]):  RDD[(String, Long)] = {

    processedDataRDD
      .mapPartitions { iter =>
        iter.map { processedData =>

          // 1. 获取 IP
          val ip: String = processedData.remoteAddr

          // 2. 返回二元组
          ip -> 1L
        }
      }
      // 3. 聚合
      .reduceByKey(_ + _)
  }

}
