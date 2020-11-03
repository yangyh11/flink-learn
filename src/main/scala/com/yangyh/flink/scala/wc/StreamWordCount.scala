package com.yangyh.flink.scala.wc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 流处理-单词计数
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    // 1.创建流计算环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 3.读取数据 Linux上启动socket流命令：nc -lk 8888
    val socketData: DataStream[String] = env.socketTextStream("node4", 8888)

    // 4.转换和处理数据
    val wordAndCount: DataStream[(String, Int)] = socketData.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0) // 分组算子
      .sum(1) // 聚合累加算子

    // 5.打印结果(Sink)
    wordAndCount.print()

    // 6.启动流计算程序
    env.execute("流处理单词计数")
  }
}
