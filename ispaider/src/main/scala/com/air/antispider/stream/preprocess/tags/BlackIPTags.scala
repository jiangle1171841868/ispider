package com.air.antispider.stream.preprocess.tags

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

/**
  * 黑名单IP打标签 -> 根据MySQL中获取的黑名单IP,通过IP匹配 -> 对数据打标签-> 匹配到就设置标签为true
  */
object BlackIPTags {

  def getBlackIPStatus(remoteAddr: String, broadcast: Broadcast[ArrayBuffer[String]]):Boolean = {

    // 1. 设置状态值 true代表 -> 黑名单IP
    var blackStatus = false

    // 1. 获取规则集合
    val blackIPRules: ArrayBuffer[String] = broadcast.value

    // 2. 遍历集合,进行匹配
    for (rule <- blackIPRules) {

      // 判断是否是黑名单IP
      if (rule.equals(remoteAddr)) {

        // 设置状态值为true
        blackStatus = true
      }
    }
    // 返回状态值
    blackStatus
  }

}
