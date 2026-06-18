package mysqlsink

import objectclass.date_behavior_count
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.{Connection, DriverManager, PreparedStatement}

class BehaviorSQLSink (table:String) extends RichSinkFunction[date_behavior_count] with Serializable {
  var conn:Connection = _
  var ps:PreparedStatement = _
  val user = "root"
  val password = "123456"
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/fk_shop?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, password)
  }

  override def invoke(value: date_behavior_count): Unit = {
    try {
      val sql = s"insert into $table value(?,?,?) on duplicate key update count=?"
      ps = conn.prepareStatement(sql)
      ps.setString(1, value.getDate())
      ps.setString(2, value.getBehavior())
      ps.setDouble(3, value.getCount())
      ps.setDouble(4, value.getCount())
      ps.executeUpdate()
    } catch {
      case e: Exception => println(s"写入失败: ${e.getMessage}")
    } finally {
      if (ps != null) ps.close()
    }
  }

  override def close(): Unit = {
    if (conn != null) conn.close()
  }
}