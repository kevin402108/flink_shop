package flinkclass

import mysqlsink.BehaviorSQLSink
import objectclass.date_behavior_count
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import java.util.Properties

object BehaviorCountToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pro = new Properties()
    pro.setProperty("bootstrap.servers", "hadoop23020201:9092,hadoop23020202:9092,hadoop23020203:9092")
    pro.setProperty("group.id", "fk_shop_behavior")
    pro.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), pro))

    // 统计每种行为类型的数量
    val data = stream.map(x => x.split(","))
      .filter(x => x.nonEmpty && x.length == 11)
      .map(x => ((x(10), x(7)), 1.0)) // (日期, 行为类型), 数量1
      .keyBy(_._1)
      .timeWindow(Time.minutes(1))
      .sum(1)

    // 写入MySQL
    data.map(x => new date_behavior_count(x._1._1, x._1._2, x._2))
      .addSink(new BehaviorSQLSink("behavior_count"))

    env.execute("Behavior Count Statistics")
  }
}