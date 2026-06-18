package flinkclass

import mysqlsink.StoreVisitSQLSink
import objectclass.date_store_visit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import java.util.Properties

object StoreVisitToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pro = new Properties()
    pro.setProperty("bootstrap.servers", "hadoop23020201:9092,hadoop23020202:9092,hadoop23020203:9092")
    pro.setProperty("group.id", "fk_shop_store_visit")
    pro.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), pro))

    // 统计每个门店的总访问量（所有行为都算访问）
    val data = stream.map(x => x.split(","))
      .filter(x => x.nonEmpty && x.length == 11)
      .map(x => ((x(10), x(6)), 1.0)) // (日期, 门店ID), 访问量1
      .keyBy(_._1)
      .timeWindow(Time.minutes(1))
      .sum(1)

    // 写入MySQL
    data.map(x => new date_store_visit(x._1._1, x._1._2, x._2))
      .addSink(new StoreVisitSQLSink("store_visit"))

    env.execute("Store Visit Statistics")
  }
}