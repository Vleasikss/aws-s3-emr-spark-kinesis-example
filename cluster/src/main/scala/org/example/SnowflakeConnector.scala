package org.example

import java.sql.{Connection, DriverManager}

object SnowflakeConnector {

  private val user = "snowflakr-account-username"
  private val password = "snowflake-account-password"
  private val account = "snowflake-account-id"
  private val db = "snowflake-database-name"
  private val schema = "snowflake-schema-name"
  private val warehouse = "snowflake-warehouse-name"
  private val role = "SYSADMIN"


  private def url: String =
    s"jdbc:snowflake://$account.snowflakecomputing.com/?db=$db&warehouse=$warehouse&schema=$schema&role=$role"


  println(url)

  def getConnection: Connection =
    DriverManager.getConnection(url, user, password)


}
