package base

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object WordCountScala {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tool = ParameterTool.fromArgs(args)
    val hostname = tool.get("hostname")
    val port = tool.getInt("port")

    val source = env.socketTextStream(hostname, port)

    val result = source.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .sum(1)


    result.print()

    env.execute("WordCount")
  }
}
