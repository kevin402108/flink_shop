package flinkclass

import mysqlsink.SaleSQLSink
import objectclass.StringAndDouble
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.util.Properties

object SaleVolumn {
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
      .filter(x => x.nonEmpty && x.length == 11)
      .map{x => if(x(7).contains("buy"))(x(x.length-1),(x(5).toDouble,1.0))
      else (x(x.length-1),(0.0,1.0))}
      .keyBy(0).timeWindow(Time.minutes(1))
      .reduce((x,y)=>(x._1,(x._2._1+y._2._1,x._2._2+y._2._2)))
    //保存每日销售额到MySQL表
    data.map(x => new StringAndDouble(x._1,x._2._1)).addSink(new SaleSQLSink("salevolume"))
    //保存每日点击流到MySQL表
    data.map(x => new StringAndDouble(x._1,x._2._2)).addSink(new SaleSQLSink("visitcount_everyday"))
    env.execute()
  }
}