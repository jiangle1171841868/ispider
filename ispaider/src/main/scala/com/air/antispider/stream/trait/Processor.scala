package com.air.antispider.stream.`trait`

import com.air.antispider.stream.common.bean.Message
import org.apache.spark.rdd.RDD

/**
  * RDD处理接口
  */
trait Processor {

  def processData(rdd: RDD[Message])

}
