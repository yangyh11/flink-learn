package com.yangyh.flink.scala.wc

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 批处理-单词计数
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2.从文件中读取数据
    val lines: DataSet[String] = env.readTextFile("./data/words")

    // 3.分词之后做count
    val wordAndCount: AggregateDataSet[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // 4.结果打印出来
    wordAndCount.print()
  }

}
