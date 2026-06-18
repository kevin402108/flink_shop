package mysqlsink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import objectclass.StringAndDouble
import java.sql.{Connection, DriverManager, PreparedStatement}

class SaleSQLSink (table:String) extends RichSinkFunction[StringAndDouble] with Serializable {
  var conn:Connection = _
  var ps:PreparedStatement = _
  val user = "root"
  val password = "123456"
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/fk_shop"

  override def open(parameters: Configuration): Unit = {
    Class.forName(driver)
    conn = DriverManager.getConnection(url, user, password)
    val sql = s"INSERT INTO $table VALUES(?,?) ON DUPLICATE KEY UPDATE ${if(table == "salevolume") "salevolume=VALUES(salevolume)" else "visitcount=VALUES(visitcount)"}"
    ps = conn.prepareStatement(sql)
  }

  override def invoke(value: StringAndDouble): Unit = {
    ps.setString(1, value.getDate())
    ps.setDouble(2, value.getSale())
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (ps != null) ps.close()
    if (conn != null) conn.close()
  }
}