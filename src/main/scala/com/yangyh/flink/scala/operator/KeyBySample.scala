package com.yangyh.flink.scala.operator

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object KeyBySample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[(String, String)] = env.fromElements(("湖北", "武汉"), ("湖南", "长沙"), ("湖北", "荆州"))

    val keyBy: KeyedStream[(String, String), Tuple] = value.keyBy(0)

    keyBy.print()

    env.execute()


  }

}
