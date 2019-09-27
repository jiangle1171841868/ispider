package com.air.antispider.stream.preprocess.filter

import com.air.antispider.stream.common.bean.Message
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

object URLFilter {

  def urlFilter(message: Message, urlFilterRulesBroadcast: Broadcast[ArrayBuffer[String]]): Boolean = {

    // 1. 从广播变量中取出过滤规则
    val urlFilterRules: ArrayBuffer[String] = urlFilterRulesBroadcast.value

    // 2. 获取ur
    val url: String = message.request
    // POST /B2C40/dist/main/images/loadingimg.jpg HTTP/1.1

    // 3. 遍历过滤规则 -> 里面是正则表达式
    for (rule <- urlFilterRules) {

      // 使用正则表达式判断url中是否包含规则,包含就过滤掉
      if (url.matches(rule)) {
        // url符合任何一个过滤规则就直接return false 结束程序
        return false;
      }
    }

    // 如果url不符合过滤规则,就是需要的数据 -> 返回true
    true
  }

}
