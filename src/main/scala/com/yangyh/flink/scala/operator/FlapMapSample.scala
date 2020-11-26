package com.yangyh.flink.scala.operator

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment,_}

object FlapMapSample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val line: DataStream[String] = env.socketTextStream("node4", 9999)
    val flapMap: DataStream[String] = line.flatMap(_.split(" "))
    flapMap.print()

    env.execute();
  }

}
