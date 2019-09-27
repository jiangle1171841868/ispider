package com.itheima.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTest {

  val CHECK_POINT_PATH: String = "data/spark-streaming/checkpoint/test-001"

  def main(args: Array[String]): Unit = {

    // 1. 构建StreamingContext实例对象
    val ssc =
      StreamingContext.getActiveOrCreate(
        CHECK_POINT_PATH,
        () => {
          // a. 创建sparkConf对象
          val sparkConf = new SparkConf()
            .setMaster("local[3]")
            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
            // 设置每秒钟每分区的最大读取量
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
          // b. 创建StreamingContext对象
          val streamingContext = new StreamingContext(sparkConf, Seconds(5))

          // 设置检查点
          streamingContext.checkpoint(CHECK_POINT_PATH)

          // 调用方法处理数据
          processData(streamingContext)

          // c. 返回
          streamingContext
        }
      )

    // 4. 启动流式应用
    ssc.start()
    ssc.awaitTermination()

    // 5. 优雅的停止
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }
    /// TODO: 抽取函数 传递ssc处理对象处理数据

    def processData(ssc: StreamingContext) = {

      // 2. 从kafka获取数据
      // a. 设置kafka连接信息
      val kafkaParams: Map[String, String] = Map(
        "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
        // 设置获取最新偏移量 -> 默认就是最新
        "auto.offset.reset" -> "largest"
      )
      // b. 设置消费的topic
      val topics: Set[String] = Set("collect_log")

      val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

      // 3. 输出数据
      kafkaDStream.foreachRDD { (rdd, time) =>

        val batchTime: String = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(time.milliseconds)

        println("-------------------------------------------")
        println(s"Time: $batchTime")
        println("-------------------------------------------")

        //a.先判断rdd是否为空
        if (!rdd.isEmpty()) {
          rdd
            //b.考虑降低分区数
            .coalesce(1)
            .foreachPartition { datas =>
              datas.foreach(println)
            }
        }
      }
    }

}
