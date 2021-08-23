package org.example

import java.sql.{Connection, DriverManager}
import java.util.Properties

object SnowflakeConnector {

  val properties = new Properties()
  properties.load(getClass.getResourceAsStream("/application.properties"))

  private val user = properties.getProperty("snowflake.account.username")
  private val password = properties.getProperty("snowflake.account.password")
  private val account = properties.getProperty("snowflake.account.id")
  private val db = properties.getProperty("snowflake.database.name")
  private val schema = properties.getProperty("snowflake.schema.name")
  private val warehouse = properties.getProperty("snowflake.warehouse.name")
  private val role = properties.getProperty("snowflake.role")


  private def url: String =
    s"jdbc:snowflake://$account.snowflakecomputing.com/?db=$db&warehouse=$warehouse&schema=$schema&role=$role"


  println(url)

  def getConnection: Connection =
    DriverManager.getConnection(url, user, password)


}
