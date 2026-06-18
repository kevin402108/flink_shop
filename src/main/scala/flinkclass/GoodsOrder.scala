package flinkclass

import java.util.Properties
import objectclass.StringAndDouble
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

object GoodsOrder {
  def main(args: Array[String]): Unit = {
    // 创建上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 配置Kafka集群连接参数
    val pro = new Properties()
    pro.setProperty("bootstrap.servers", "hadoop23020201:9092,hadoop23020202:9092,hadoop23020203:9092")
    pro.setProperty("group.id", "fk_shop")
    // 创建数据源
    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), pro))
    // 数据处理过程
    val data = stream.map(x => x.split(","))
      .filter(x => x.nonEmpty && x.length == 11 && x(7).contains("buy"))
      .map(x => new StringAndDouble(x(4), 1.0))
      .keyBy(x => x.getDate())
      .timeWindow(Time.minutes(1))
      .process(new ProcessWindowFunction[StringAndDouble, String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[StringAndDouble], out: Collector[String]): Unit = {
          val getsum = elements.toList.groupBy(x => x.getDate())
            .map { x => val y = x._2.map(x => x.getSale()).sum; (x._1, y) }
          val e = getsum.toList.sortBy(x => x._2).reverse.take(10)
          e.map(x => out.collect(System.currentTimeMillis().toString + ":" + x._1 + ":" + x._2))
        }
      })

    // 保存商品销量Top10到MySQL表
    data.print()
    env.execute()
  }
}