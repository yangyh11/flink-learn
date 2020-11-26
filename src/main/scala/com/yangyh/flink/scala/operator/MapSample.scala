package com.yangyh.flink.scala.operator

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object MapSample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = env.socketTextStream("node4", 9999)

    val map: DataStream[String] = line.map(_ + "#")

    map.print()

    env.execute()

  }

}
