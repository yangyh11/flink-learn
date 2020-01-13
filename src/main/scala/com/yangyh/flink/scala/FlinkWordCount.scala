package com.yangyh.flink.scala

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object FlinkWordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val lines: DataSet[String] = env.readTextFile("./data/words")
    val words: DataSet[String] = lines.flatMap(line=>{line.split(" ")})
    val map: DataSet[(String, Int)] = words.map(word=>{(word,1)})
//    map.groupBy(0).sum(1).print()
    map.groupBy(0).sum(1).writeAsText("./data/result2",WriteMode.OVERWRITE)
    env.execute()

  }

}
