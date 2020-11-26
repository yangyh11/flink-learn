package com.yangyh.flink.scala.operator

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object ReduceSample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[(String, String, Integer)] = env.fromElements(("湖北", "武汉", 1), ("湖南", "长沙", 1), ("湖北", "荆州", 1))

    val keyBy: KeyedStream[(String, String, Integer), Tuple] = source.keyBy(0)
    val reduceResult: DataStream[(String, String, Integer)] = keyBy.reduce((t1, t2) => {
      (t1._1, t1._2 + "&" + t2._2, t1._3 + t2._3)
    })

    reduceResult.print()

    env.execute()

  }

}
