package flinkclass

import mysqlsink.GoodsSQLSink
import objectclass.date_goods_sale
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import java.util.Properties

object GoodsSaleToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val pro = new Properties()
    pro.setProperty("bootstrap.servers", "hadoop23020201:9092,hadoop23020202:9092,hadoop23020203:9092")
    pro.setProperty("group.id", "fk_shop_goods")
    pro.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), pro))

    // 统计每个商品的购买次数
    val data = stream.map(x => x.split(","))
      .filter(x => x.nonEmpty && x.length == 11 && x(7) == "buy")
      .map(x => ((x(10), x(4)), 1.0)) // (日期, 商品ID), 销量1
      .keyBy(_._1)
      .timeWindow(Time.minutes(1))
      .sum(1)

    // 写入MySQL
    data.map(x => new date_goods_sale(x._1._1, x._1._2, x._2))
      .addSink(new GoodsSQLSink("goods_sale"))

    env.execute("Goods Sale Statistics")
  }
}