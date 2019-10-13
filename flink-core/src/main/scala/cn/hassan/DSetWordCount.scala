package cn.hassan

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

object DSetWordCount {

  def main(args: Array[String]): Unit = {
    // 1 env 2 source 3 transform 4 sink
    val env = ExecutionEnvironment.getExecutionEnvironment
    val path = Thread.currentThread().getContextClassLoader.getResource("data.txt").getPath
    val dataSet = env.readTextFile(path)
    val result = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    result.print()
  }
}
