package org.example

import java.sql.{Connection, DriverManager}

object SnowflakeConnector {

  private val user = "VLAS"
  private val password = "LPxd601Kafhntkfr"
  private val account = "HIA90451"
  private val db = "TEST_DB"
  private val schema = "PUBLIC"
  private val warehouse = "TEST_WH"
  private val role = "SYSADMIN"

  private def url: String =
    s"jdbc:snowflake://$account.snowflakecomputing.com/?db=$db&warehouse=$warehouse&schema=$schema&role=$role"


  def getConnection: Connection =
    DriverManager.getConnection(url, user, password)


}
