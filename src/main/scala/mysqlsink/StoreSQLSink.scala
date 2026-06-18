package mysqlsink

import objectclass.date_store_sale
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.{Connection, DriverManager, PreparedStatement}

class StoreSQLSink (table:String) extends RichSinkFunction[date_store_sale] with Serializable {
  var conn:Connection = _
  var ps:PreparedStatement = _
  val user = "root"
  val password = "123456"
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/fk_shop"

  override def open(parameters: Configuration): Unit = {
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, password)
    val sql = s"INSERT INTO $table VALUES(?,?,?)"
    ps = conn.prepareStatement(sql)
  }

  override def invoke(value: date_store_sale): Unit = {
    ps.setString(1, value.getDate())
    ps.setString(2, value.getStore())
    ps.setDouble(3, value.getSale())
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }
}