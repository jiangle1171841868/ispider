package com.air.antispider.stream.blackprcess

import java.util

import com.air.antispider.stream.common.bean.ProcessedData
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 统计关键页面访问的最小时间间隔
  */
object CriticalPagesAccessMinInterval {

  def processData(rdd: RDD[ProcessedData], criticalPagesRulesBroadcast: Broadcast[ArrayBuffer[String]]):RDD[(String,Long)] = {

    // 1. 获取广播变量的值
    val criticalPagesRules: ArrayBuffer[String] = criticalPagesRulesBroadcast.value

    // 2. 过滤出关键页面
    rdd.filter { data =>
      // a. 获取URL
      val url = data.request
      // b. 设置状态值, 接收判断状态
      var flag = false

      // b. 遍历规则
      for (criticalPagesRule <- criticalPagesRules) {

        // c. 判断是否是关键规则
        if (url.matches(criticalPagesRule)) {

          // 是关键规则 -> flat=true
          flag = true
        }
      }
      // 返回
      flag
    }

      // 3. 对关键页面数据处理 -> 返回IP -> 时间戳 二元组
      .mapPartitions { iter =>
      iter
        .map { data =>
          // a. 获取URL
          val ip = data.remoteAddr
          // b. 获取时间 -> String
          val timeStr = data.timeIso8601
          // c. 将时间转化为  -> 时间戳 Long  时间格式 --> 1990-01-01T12:12:01+8:00
          /*
                 - 注意: 时间格式中间有字符串T 解决:
                 - 1. 使用字符传替换将T 替换成空格
                 - 2. T用单引号
           */

          val time: Long = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss").parse(timeStr).getTime

          // d. 返回IP 和 时间戳
          ip -> time
        }
    }
      // 4. 聚合
      .groupByKey()

      // 5. 对时间戳排序,求最小时间间隔
      .map { case (ip, iter) =>

      // a. 将迭代器转化为数组,进行升序排序
      val arr: Array[Long] = iter.toArray
      util.Arrays.sort(arr)

      // 定义set集合 用来收集时间差 -> 去重
      val set = new mutable.HashSet[Long]()

      // b. 遍历数组的索引,获取时间差
      for (index <- 0 until arr.length - 1) {

        // i. 获取当前索引的值
        val currentTime = arr(index)
        //println(currentTime)

        // ii. 获取下一个值
        val nextTime = arr(index + 1)
        //println(nextTime)

        // iii. 求差值
        val diffTime = nextTime - currentTime

        // set收集
        set += diffTime
      }
      val intervalArr = set.toArray
      util.Arrays.sort(intervalArr)

      // 判断数组长度是否大于0 大于0就取第一个值 -> 最小值
      if (intervalArr.length > 0) {

        (ip, intervalArr(0))

      } else {

        // 没有时间差 返回-1
        (ip, -1)
      }
    }

  }
}
