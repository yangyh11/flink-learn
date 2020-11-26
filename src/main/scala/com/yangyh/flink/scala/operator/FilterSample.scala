package com.yangyh.flink.scala.operator

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object FilterSample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = env.socketTextStream("node4", 9999)
    val filter: DataStream[String] = line.filter(_.endsWith("java"))
    filter.print()

    env.execute()
  }

}
