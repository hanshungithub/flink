package cn.hassan

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.api.scala._

object DStreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dStream = env.socketTextStream("127.0.0.1",7777)
    val result = dStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    result.print()
    env.execute()
  }
}
