package mysqlsink

import objectclass.date_store_visit
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.{Connection, DriverManager, PreparedStatement}

class StoreVisitSQLSink (table:String) extends RichSinkFunction[date_store_visit] with Serializable {
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

  override def invoke(value: date_store_visit): Unit = {
    try {
      val sql = s"insert into $table value(?,?,?) on duplicate key update visits=?"
      ps = conn.prepareStatement(sql)
      ps.setString(1, value.getDate())
      ps.setString(2, value.getStore())
      ps.setDouble(3, value.getVisits())
      ps.setDouble(4, value.getVisits())
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