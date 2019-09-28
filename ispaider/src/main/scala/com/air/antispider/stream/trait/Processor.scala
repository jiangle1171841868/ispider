package com.air.antispider.stream.`trait`

import com.air.antispider.stream.common.bean.{Message, ProcessedData}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * RDD处理接口
  */
trait Processor {

  def processData(processedDataRDD: RDD[ProcessedData]):RDD[_]

}
