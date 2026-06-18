package flinkclass

import mysqlsink. StoreSQLSink
import objectclass.date_store_sale
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import java.util.Properties

object StoreSale {
  def main(args: Array[String]): Unit = {
    // 创建上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 配置Kafka集群连接参数
    val pro = new Properties()
    pro.setProperty("bootstrap.servers","hadoop23020201:9092,hadoop23020202:9092,hadoop23020203:9092")
    pro.setProperty("group.id","fk_shop")
    // 创建数据源
    val stream = env.addSource(
      new FlinkKafkaConsumer[String]("shop",new SimpleStringSchema(),pro))
    //数据处理过程
    val data = stream.map(x => x.split(","))
      .filter(x => x.nonEmpty && x.length == 11 && x(7).contains("buy"))
      .map(x => ((x(x.length-1),x(6)),x(5).toDouble))
      .keyBy(0).timeWindow(Time.minutes(1)).sum(1)
    //保存每日销售额到MySQL表
    data.map(x => new date_store_sale(x._1._1,x._1._2,x._2)).addSink(new StoreSQLSink("store_sale"))
    env.execute()
  }
}