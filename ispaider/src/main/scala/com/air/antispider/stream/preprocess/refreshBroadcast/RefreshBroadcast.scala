package com.air.antispider.stream.preprocess.refreshBroadcast

import com.air.antispider.stream.common.bean.AnalyzeRule
import com.air.antispider.stream.common.util.jedis.JedisConnectionUtil
import com.air.antispider.stream.preprocess.rule.AnalyzeRuleDB
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

object RefreshBroadcast {

  /**
    * 根据redis中的状态值类判断MySQL中的规则是否修改 -> 重新获取规则,更新广播变量
    *
    * @param sc
    * @param broadcast
    * @param signKey => 在redis保存mysql更改状态的key
    * @return
    */
  def refreshBroadcast(sc: SparkContext, broadcast: Broadcast[ArrayBuffer[String]], signKey: String): Broadcast[ArrayBuffer[String]] = {

    var urlFilterRulesBroadcast: Broadcast[ArrayBuffer[String]] = broadcast

    // 1. 从redis中设置一个标记  ->  key -> 来判断MySQL数据库规则是否修改 -> true表示修改
    // a. 获取redis集群连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // b. 获取标记key的值
    var signValue: String = jedis.get(signKey)

    // c. 第一次获取的话没有值,赋初始值,存入redis
    if (StringUtils.isBlank(signValue)) {

      //赋初始值为true  -> 表示修改 -> 需要去获取规则
      signValue = "true"
      // 将数据设置到redis中
      jedis.set(signKey, signValue)
    }

    // 2. 判断状态值是否为true -> 是true说明规则改了 ->  需要更新广播变量
    if (signValue.toBoolean) {

      // a. 释放广播变量
      broadcast.unpersist()

      // b. 从MySQL数据库获取规则
      val urlFilterRules: ArrayBuffer[String] = AnalyzeRuleDB.getFilterRuleList()

      // c. 更新广播变量
      urlFilterRulesBroadcast = sc.broadcast(urlFilterRules)

      // d. 将状态值设置为false
      signValue = "false"
      jedis.set(signKey, signValue)

    }
    // e. 返回广播变量
    urlFilterRulesBroadcast
  }

  /**
    * 根据redis中的状态值类判断MySQL中的规则是否修改 -> 重新获取规则,更新广播变量
    *
    * @param sc
    * @param broadcast
    * @param signKey => 在redis保存mysql更改状态的key
    * @return
    */
  def refreshRequestRuleBroadcast(sc: SparkContext, broadcast: Broadcast[Map[String, ArrayBuffer[String]]], signKey: String): Broadcast[Map[String, ArrayBuffer[String]]] = {

    var requestRuleBroadcast: Broadcast[Map[String, ArrayBuffer[String]]] = broadcast

    // 1. 从redis中设置一个标记  ->  key -> 来判断MySQL数据库规则是否修改 -> true表示修改
    // a. 获取redis集群连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // b. 获取标记key的值
    var signValue: String = jedis.get(signKey)

    // c. 第一次获取的话没有值,赋初始值,存入redis
    if (StringUtils.isBlank(signValue)) {

      //赋初始值为true  -> 表示修改 -> 需要去获取规则
      signValue = "true"
      // 将数据设置到redis中
      jedis.set(signKey, signValue)
    }

    // 2. 判断状态值是否为true -> 是true说明规则改了 ->  需要更新广播变量
    if (signValue.toBoolean) {

      // a. 释放广播变量
      broadcast.unpersist()

      // b. 从MySQL数据库获取规则
      val urlFilterRules: Map[String, ArrayBuffer[String]] = AnalyzeRuleDB.getClassifyRule()

      // c. 更新广播变量
      requestRuleBroadcast = sc.broadcast(urlFilterRules)

      // d. 将状态值设置为false
      signValue = "false"
      jedis.set(signKey, signValue)

    }
    // e. 返回广播变量
    requestRuleBroadcast
  }

  /**
    * 根据redis中的状态值类判断MySQL中的规则是否修改 -> 重新获取规则,更新广播变量
    *
    * @param sc
    * @param broadcast
    * @param signKey => 在redis保存mysql更改状态的key
    * @return
    */
  def refreshQueryRulesBroadcast(sc: SparkContext, broadcast: Broadcast[List[AnalyzeRule]], signKey: String): Broadcast[List[AnalyzeRule]] = {

    var queryRulesBroadcast: Broadcast[List[AnalyzeRule]] = broadcast

    // 1. 从redis中设置一个标记  ->  key -> 来判断MySQL数据库规则是否修改 -> true表示修改
    // a. 获取redis集群连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // b. 获取标记key的值
    var signValue: String = jedis.get(signKey)

    // c. 第一次获取的话没有值,赋初始值,存入redis
    if (StringUtils.isBlank(signValue)) {

      //赋初始值为true  -> 表示修改 -> 需要去获取规则
      signValue = "true"
      // 将数据设置到redis中
      jedis.set(signKey, signValue)
    }

    // 2. 判断状态值是否为true -> 是true说明规则改了 ->  需要更新广播变量
    if (signValue.toBoolean) {

      // a. 释放广播变量
      broadcast.unpersist()

      // b. 从MySQL数据库获取规则
      val queryRules: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(0)

      // c. 更新广播变量
      queryRulesBroadcast = sc.broadcast(queryRules)


      // d. 将状态值设置为false
      signValue = "false"
      jedis.set(signKey, signValue)

    }
    // e. 返回广播变量
    queryRulesBroadcast
  }

  /**
    * 根据redis中的状态值类判断MySQL中的规则是否修改 -> 重新获取规则,更新广播变量
    *
    * @param sc
    * @param broadcast
    * @param signKey => 在redis保存mysql更改状态的key
    * @return
    */
  def refreshBookRulesBroadcast(sc: SparkContext, broadcast: Broadcast[List[AnalyzeRule]], signKey: String): Broadcast[List[AnalyzeRule]] = {

    var bookRulesBroadcast: Broadcast[List[AnalyzeRule]] = broadcast

    // 1. 从redis中设置一个标记  ->  key -> 来判断MySQL数据库规则是否修改 -> true表示修改
    // a. 获取redis集群连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // b. 获取标记key的值
    var signValue: String = jedis.get(signKey)

    // c. 第一次获取的话没有值,赋初始值,存入redis
    if (StringUtils.isBlank(signValue)) {

      //赋初始值为true  -> 表示修改 -> 需要去获取规则
      signValue = "true"
      // 将数据设置到redis中
      jedis.set(signKey, signValue)
    }

    // 2. 判断状态值是否为true -> 是true说明规则改了 ->  需要更新广播变量
    if (signValue.toBoolean) {

      // a. 释放广播变量
      broadcast.unpersist()

      // b. 从MySQL数据库获取规则
      val bookRules: List[AnalyzeRule] = AnalyzeRuleDB.queryRule(1)

      // c. 更新广播变量
      bookRulesBroadcast = sc.broadcast(bookRules)

      // d. 将状态值设置为false
      signValue = "false"
      jedis.set(signKey, signValue)

    }
    // e. 返回广播变量
    bookRulesBroadcast
  }

  /**
    * 根据redis中的状态值类判断MySQL中的规则是否修改 -> 重新获取规则,更新广播变量
    *
    * @param sc
    * @param broadcast
    * @param signKey => 在redis保存mysql更改状态的key
    * @return
    */
  def refreshBlackIPRulesBroadcast(sc: SparkContext, broadcast: Broadcast[ArrayBuffer[String]], signKey: String): Broadcast[ArrayBuffer[String]] = {

    var blackIPRulesBroadcast: Broadcast[ArrayBuffer[String]] = broadcast

    // 1. 从redis中设置一个标记  ->  key -> 来判断MySQL数据库规则是否修改 -> true表示修改
    // a. 获取redis集群连接
    val jedis: JedisCluster = JedisConnectionUtil.getJedisCluster

    // b. 获取标记key的值
    var signValue: String = jedis.get(signKey)

    // c. 第一次获取的话没有值,赋初始值,存入redis
    if (StringUtils.isBlank(signValue)) {

      //赋初始值为true  -> 表示修改 -> 需要去获取规则
      signValue = "true"
      // 将数据设置到redis中
      jedis.set(signKey, signValue)
    }

    // 2. 判断状态值是否为true -> 是true说明规则改了 ->  需要更新广播变量
    if (signValue.toBoolean) {

      // a. 释放广播变量
      broadcast.unpersist()

      // b. 从MySQL数据库获取规则
      val blackIpRules: ArrayBuffer[String] = AnalyzeRuleDB.queryBlackIp()

      // c. 更新广播变量
      blackIPRulesBroadcast = sc.broadcast(blackIpRules)

      // d. 将状态值设置为false
      signValue = "false"
      jedis.set(signKey, signValue)

    }
    // e. 返回广播变量
    blackIPRulesBroadcast
  }
}
