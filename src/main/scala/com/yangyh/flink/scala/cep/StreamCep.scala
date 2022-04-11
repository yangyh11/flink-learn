//package com.yangyh.flink.scala.cep
//
//import org.apache.flink.cep.scala.{CEP, PatternStream}
//import org.apache.flink.cep.scala.pattern.Pattern
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//
//case class LoginEvent(id : String, status : String)
//
//object StreamCep {
//
//  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    import org.apache.flink.streaming.api.scala._
//
//    val socketData: DataStream[String] = env.socketTextStream("node4", 8888)
//
//    val mapStream: DataStream[LoginEvent] = socketData.map(line => {
//      val values = line.split(",")
//      val id = values(0)
//      val status = values(1)
//      LoginEvent(id, status)
//    })
//
//    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
//      .where(_.status.equals("1"))
//      .followedBy("退出")
//      .where(_.status.equals("2"))
//
//    val patternStream: PatternStream[LoginEvent] = CEP.pattern(mapStream, pattern)
//
//
//    env.execute("cep")
//
//
//  }
//
//}
